package com.metroride.fare.consumer;

import com.metroride.fare.FareServiceApplication;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.pricing.FareQuoteException;
import com.metroride.fare.processing.ProcessedEventRecorder;
import com.metroride.fare.processing.ProcessedEventRecorder.Outcome;
import com.metroride.fare.processing.ProcessedEventRecorder.Result;
import io.lettuce.core.Consumer;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.PendingMessage;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;

/**
 * Consumer-group reader for {@code events.ride.assignments}, structured like {@code consume()} in
 * {@code services/dispatch-service/cmd/main.go}, plus the pending-entry recovery that the Go
 * consumers do not have:
 *
 * <ol>
 *   <li>{@code XGROUP CREATE ... 0 MKSTREAM} at startup; an existing group is fine.</li>
 *   <li>Loop: every {@code reclaim-interval}, one
 *       {@code XAUTOCLAIM stream group consumer min-idle 0-0 COUNT n}; every claimed entry goes
 *       through {@link #handle} like a new one. Then
 *       {@code XREADGROUP GROUP g c COUNT n BLOCK t STREAMS stream >}.</li>
 *   <li>Per entry: decode the envelope, record it (and for {@code ride_assigned}, quote the fare
 *       and append the ledger entry) in one PostgreSQL transaction, then {@code XACK}.</li>
 *   <li>Per failed entry: {@link FailureClass#of classify} the failure. A poison entry is
 *       dead-lettered at once. A retryable one is left pending for the next reclaim pass unless
 *       its {@link StreamEntryAge age} already exceeds {@code retry-budget}, in which case it is
 *       dead-lettered too. Dead-lettering is {@code XADD} to {@code events.dead_letter} first and
 *       {@code XACK} of the original entry only after Redis confirmed the {@code XADD}; when the
 *       {@code XADD} fails the entry stays pending and the next reclaim pass tries again, so a
 *       dead letter can be published twice. {@code original_event_id} is the key to deduplicate on.</li>
 * </ol>
 *
 * <p>Everything above runs on one thread over one dedicated Lettuce connection: the blocking read
 * never stalls the shared connection the readiness check uses, the reclaim pass never runs
 * concurrently with the read loop, and {@code ProcessedEventRecorder.record} keeps its single-writer
 * concurrency model. The reclaim pass is in this class rather than on a scheduler for that reason.
 *
 * <p>Reclaim, acknowledgement and dead-lettering take the stream from the message itself
 * ({@link StreamMessage#getStream()}), so a second stream offset in the read can be added without
 * touching any of the three.
 */
@Component
public class RideAssignmentConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RideAssignmentConsumer.class);

    /** Start of the {@code XAUTOCLAIM} scan: the whole pending entry list, every pass. */
    private static final String RECLAIM_FROM_START = "0-0";

    private final ConsumerProperties properties;
    private final LettuceConnectionFactory connectionFactory;
    private final EnvelopeCodec codec;
    private final ProcessedEventRecorder recorder;
    private final DeadLetterPublisher deadLetters;
    private final Clock clock;
    private final MeterRegistry meterRegistry;
    private final Duration shutdownTimeout;

    private final Counter recordedEvents;
    private final Counter duplicateEvents;
    private final Counter quoteHolds;
    private final Map<FareQuoteException.Reason, Counter> quoteFailures = new EnumMap<>(FareQuoteException.Reason.class);
    private final Counter consumeErrors;
    private final Counter redisErrors;
    private final Counter postgresErrors;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;
    private Future<?> loop;
    private StatefulRedisConnection<String, String> connection;

    public RideAssignmentConsumer(
            ConsumerProperties properties,
            LettuceConnectionFactory connectionFactory,
            EnvelopeCodec codec,
            ProcessedEventRecorder recorder,
            DeadLetterPublisher deadLetters,
            Clock clock,
            MeterRegistry meterRegistry,
            @Value("${spring.lifecycle.timeout-per-shutdown-phase}") Duration shutdownTimeout) {
        this.properties = properties;
        this.connectionFactory = connectionFactory;
        this.codec = codec;
        this.recorder = recorder;
        this.deadLetters = deadLetters;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
        this.shutdownTimeout = shutdownTimeout;

        // Registered eagerly so /metrics exposes every series at zero. The Go CounterVecs with the
        // same names only materialize a label set on its first increment.
        String service = FareServiceApplication.SERVICE_NAME;
        this.recordedEvents = meterRegistry.counter("metroride.fare.events.processed",
                "service", service, "stream", properties.stream(), "outcome", "recorded");
        this.duplicateEvents = meterRegistry.counter("metroride.fare.events.processed",
                "service", service, "stream", properties.stream(), "outcome", "duplicate");
        this.quoteHolds = meterRegistry.counter("metroride.fare.quotes",
                "service", service, "kind", "quote_hold");
        for (FareQuoteException.Reason reason : FareQuoteException.Reason.values()) {
            quoteFailures.put(reason, meterRegistry.counter("metroride.fare.quote.failures",
                    "service", service, "reason", reason.label()));
        }
        this.consumeErrors = meterRegistry.counter("metroride.stream.consume.errors",
                "service", service, "stream", properties.stream());
        this.redisErrors = meterRegistry.counter("metroride.dependency.errors",
                "service", service, "dependency", "redis");
        this.postgresErrors = meterRegistry.counter("metroride.dependency.errors",
                "service", service, "dependency", "postgres");
        // The per-stream series below are looked up by the message's own stream when they are
        // incremented; registering them here for the configured stream only pins them at zero.
        reclaimedEntries(properties.stream());
        for (DeadLetterReason reason : DeadLetterReason.values()) {
            deadLettered(properties.stream(), reason);
        }
        deadLetterPublishFailures(properties.stream());
    }

    private Counter reclaimedEntries(String stream) {
        return meterRegistry.counter("metroride.fare.events.reclaimed",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream);
    }

    private Counter deadLettered(String stream, DeadLetterReason reason) {
        return meterRegistry.counter("metroride.fare.dead_letters",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream, "reason", reason.label());
    }

    private Counter deadLetterPublishFailures(String stream) {
        return meterRegistry.counter("metroride.fare.dead_letter.publish.failures",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream);
    }

    // ---- lifecycle -------------------------------------------------------------------------

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long commandTimeoutMillis = connectionFactory.getTimeout();
        if (properties.blockTimeout().toMillis() >= commandTimeoutMillis) {
            running.set(false);
            throw new IllegalStateException("metroride.consumer.block-timeout (" + properties.blockTimeout()
                    + ") must be shorter than spring.data.redis.timeout (" + commandTimeoutMillis
                    + "ms) or every blocking read would time out");
        }

        // Standalone Redis only, which is all the Go services support as well.
        RedisClient client = (RedisClient) connectionFactory.getRequiredNativeClient();
        connection = client.connect();
        ensureConsumerGroup(connection.sync());

        executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "fare-stream-consumer");
            thread.setDaemon(false);
            return thread;
        });
        loop = executor.submit(this::runLoop);
        log.atInfo()
                .addKeyValue("stream", properties.stream())
                .addKeyValue("group", properties.group())
                .addKeyValue("consumer", properties.name())
                .addKeyValue("reclaim_interval", properties.reclaimInterval().toString())
                .addKeyValue("reclaim_min_idle", properties.reclaimMinIdle().toString())
                .addKeyValue("retry_budget", properties.retryBudget().toString())
                .log("stream consumer started");
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // Let the loop finish the batch it already read: an entry that was delivered but neither
        // processed nor acknowledged would sit in the pending list until the next reclaim pass,
        // on this or another instance, picks it up.
        executor.shutdown();
        try {
            loop.get(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("stream consumer did not stop within {}; abandoning the loop", shutdownTimeout);
            loop.cancel(true);
        } catch (ExecutionException e) {
            log.error("stream consumer loop terminated with an error", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            connection.close();
            log.info("stream consumer stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    // ---- consumption -----------------------------------------------------------------------

    private void ensureConsumerGroup(RedisCommands<String, String> commands) {
        try {
            // "0" reads the stream from its beginning, as the Go ensureGroup does.
            commands.xgroupCreate(
                    XReadArgs.StreamOffset.from(properties.stream(), "0"),
                    properties.group(),
                    XGroupCreateArgs.Builder.mkstream(true));
        } catch (RedisBusyException alreadyExists) {
            // BUSYGROUP: the group is already there, which is the normal case after the first start.
        }
    }

    private void runLoop() {
        RedisCommands<String, String> commands = connection.sync();
        Consumer<String> consumer = Consumer.from(properties.group(), properties.name());
        XReadArgs readArgs = XReadArgs.Builder
                .count(properties.batchSize())
                .block(properties.blockTimeout());
        XReadArgs.StreamOffset<String> newEntries = XReadArgs.StreamOffset.lastConsumed(properties.stream());
        // The first pass runs before the first read, so whatever a previous instance left pending
        // is retried before any new entry is touched.
        Instant nextReclaim = clock.instant();

        while (running.get()) {
            if (!clock.instant().isBefore(nextReclaim)) {
                reclaim(commands, consumer, newEntries.getName());
                nextReclaim = clock.instant().plus(properties.reclaimInterval());
            }
            List<StreamMessage<String, String>> messages;
            try {
                messages = commands.xreadgroup(consumer, readArgs, newEntries);
            } catch (RedisException e) {
                if (!running.get()) {
                    break;
                }
                consumeErrors.increment();
                redisErrors.increment();
                log.atError().addKeyValue("stream", properties.stream()).setCause(e)
                        .log("read ride assignment stream failed");
                if (e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                    // The stream or group was deleted underneath us; recreate rather than spin.
                    try {
                        ensureConsumerGroup(commands);
                    } catch (RedisException recreate) {
                        log.error("recreate consumer group failed", recreate);
                    }
                }
                if (!pause(properties.errorBackoff())) {
                    break;
                }
                continue;
            }
            // The whole batch is handled even if stop() was called meanwhile; see stop().
            for (StreamMessage<String, String> message : messages) {
                handleGuarded(commands, message);
            }
        }
        log.info("stream consumer loop exited");
    }

    /**
     * One {@code XAUTOCLAIM} over the group's pending entry list for {@code stream}, then the
     * claimed entries through the normal handler. Claiming resets an entry's idle time, so an
     * entry that fails again waits another {@code reclaim-min-idle} before the next pass sees it.
     * A pass claims at most {@code batch-size} entries; a longer backlog drains one batch per
     * interval.
     */
    private void reclaim(RedisCommands<String, String> commands, Consumer<String> consumer, String stream) {
        List<StreamMessage<String, String>> claimed;
        try {
            claimed = commands.xautoclaim(stream, XAutoClaimArgs.Builder
                    .xautoclaim(consumer, properties.reclaimMinIdle(), RECLAIM_FROM_START)
                    .count(properties.batchSize()))
                    .getMessages();
        } catch (RedisException e) {
            if (!running.get()) {
                return;
            }
            redisErrors.increment();
            log.atError().addKeyValue("stream", stream).setCause(e).log("reclaim pending entries failed");
            return;
        }
        if (claimed.isEmpty()) {
            return;
        }
        Map<String, Long> deliveryCounts = deliveryCounts(commands, consumer, stream, claimed);
        Instant now = clock.instant();
        for (StreamMessage<String, String> message : claimed) {
            if (message.getBody() == null) {
                // Redis reports an entry that was trimmed out of the stream while pending with no
                // body; it is gone from the pending list as well, so there is nothing to handle.
                log.atWarn().addKeyValue("stream", stream).addKeyValue("message_id", message.getId())
                        .log("pending entry no longer in stream; dropped by XAUTOCLAIM");
                continue;
            }
            reclaimedEntries(stream).increment();
            log.atInfo()
                    .addKeyValue("stream", stream)
                    .addKeyValue("message_id", message.getId())
                    .addKeyValue("age_seconds", ageSeconds(message.getId(), now))
                    .addKeyValue("delivery_count", deliveryCounts.getOrDefault(message.getId(), -1L))
                    .log("pending entry reclaimed");
            handleGuarded(commands, message);
        }
    }

    /**
     * {@code XAUTOCLAIM} returns the entries but not their delivery counts, so one {@code XPENDING}
     * over the claimed ID range fetches them. They are logged, never used for a decision, so a
     * failed or partial lookup only costs the log field (reported as {@code -1}).
     */
    private Map<String, Long> deliveryCounts(
            RedisCommands<String, String> commands,
            Consumer<String> consumer,
            String stream,
            List<StreamMessage<String, String>> claimed) {
        Range<String> range = Range.create(claimed.get(0).getId(), claimed.get(claimed.size() - 1).getId());
        try {
            return commands.xpending(stream, consumer, range, Limit.from(claimed.size() + (long) properties.batchSize()))
                    .stream()
                    .collect(Collectors.toMap(PendingMessage::getId, PendingMessage::getRedeliveryCount, (a, b) -> a));
        } catch (RedisException e) {
            redisErrors.increment();
            log.atWarn().addKeyValue("stream", stream).setCause(e).log("read delivery counts of reclaimed entries failed");
            return Map.of();
        }
    }

    private void handleGuarded(RedisCommands<String, String> commands, StreamMessage<String, String> message) {
        try {
            handle(commands, message);
        } catch (RuntimeException e) {
            // handle() classifies every failure of the work itself. Anything that escapes comes from
            // the failure handling (a bug), and must not end this thread silently while isRunning()
            // and /readyz still look healthy. The entry stays pending for the next reclaim pass.
            consumeErrors.increment();
            log.atError().addKeyValue("message_id", message.getId()).setCause(e)
                    .log("unexpected failure handling ride assignment event; entry left pending");
        }
    }

    private void handle(RedisCommands<String, String> commands, StreamMessage<String, String> message) {
        Envelope envelope = null;
        Result result;
        try {
            envelope = codec.decode(message.getId(), message.getBody());
            result = recorder.record(message.getStream(), envelope);
        } catch (RuntimeException e) {
            onFailure(commands, message, envelope, e);
            return;
        }

        Outcome outcome = result.outcome();
        if (outcome == Outcome.RECORDED) {
            recordedEvents.increment();
        } else {
            duplicateEvents.increment();
        }
        // Counted here, after the commit, so a rolled-back transaction never counts as a quote.
        result.quoteHold().ifPresent(hold -> quoteHolds.increment());
        var entry = log.atInfo()
                .addKeyValue("message_id", message.getId())
                .addKeyValue("event_id", envelope.id())
                .addKeyValue("event_type", envelope.type())
                .addKeyValue("source", envelope.source())
                .addKeyValue("ride_id", envelope.correlationId())
                .addKeyValue("outcome", outcome.name().toLowerCase());
        if (result.quoteHold().isPresent()) {
            JournalEntry hold = result.quoteHold().get();
            entry = entry.addKeyValue("journal_kind", hold.kind().code())
                    .addKeyValue("quote", hold.postings().get(0).amount().toString());
        }
        entry.log(outcome == Outcome.RECORDED ? "event recorded" : "duplicate event skipped");

        // Acknowledge only after the transaction above has committed. A failed ack is logged and
        // the entry stays pending; its reclaimed delivery hits the conflict clause and acks as a
        // duplicate.
        acknowledge(commands, message);
    }

    /**
     * The transaction has rolled back (or never started), so nothing of this entry is recorded.
     * {@code envelope} is {@code null} when decoding is what failed.
     */
    private void onFailure(
            RedisCommands<String, String> commands,
            StreamMessage<String, String> message,
            Envelope envelope,
            RuntimeException failure) {
        count(failure);
        FailureClass failureClass = FailureClass.of(failure);
        LoggingEventBuilder entry = log.atError()
                .addKeyValue("message_id", message.getId())
                .addKeyValue("failure_class", failureClass.name().toLowerCase())
                .setCause(failure);
        if (envelope != null) {
            entry = entry.addKeyValue("event_id", envelope.id())
                    .addKeyValue("event_type", envelope.type())
                    .addKeyValue("ride_id", envelope.correlationId());
        }
        switch (failureClass) {
            case POISON -> {
                entry.log("handle event failed; dead-lettering poison entry");
                deadLetter(commands, message, envelope, failure, DeadLetterReason.POISON);
            }
            case RETRYABLE -> {
                Instant now = clock.instant();
                Optional<Duration> age = StreamEntryAge.of(message.getId(), now);
                boolean exhausted = age.map(a -> a.compareTo(properties.retryBudget()) > 0).orElse(false);
                entry = entry.addKeyValue("age_seconds", ageSeconds(message.getId(), now))
                        .addKeyValue("retry_budget", properties.retryBudget().toString());
                if (exhausted) {
                    entry.log("handle event failed; retry budget exhausted, dead-lettering entry");
                    deadLetter(commands, message, envelope, failure, DeadLetterReason.RETRY_BUDGET_EXHAUSTED);
                } else {
                    entry.log("handle event failed; entry left pending for the next reclaim pass");
                }
            }
        }
    }

    private void count(RuntimeException failure) {
        if (failure instanceof FareQuoteException quote) {
            quoteFailures.get(quote.reason()).increment();
        } else if (failure instanceof DataAccessException || failure instanceof TransactionException) {
            postgresErrors.increment();
        } else {
            // EnvelopeDecodeException, and anything unforeseen.
            consumeErrors.increment();
        }
    }

    /**
     * Dead letter first, acknowledge second. If the {@code XADD} is not confirmed the entry stays
     * pending and is dead-lettered again on a later pass; if the {@code XACK} fails after a
     * confirmed {@code XADD} the same happens and the dead letter is duplicated. Both are preferable
     * to acknowledging an entry whose dead letter never landed.
     */
    private void deadLetter(
            RedisCommands<String, String> commands,
            StreamMessage<String, String> message,
            Envelope envelope,
            RuntimeException cause,
            DeadLetterReason reason) {
        String stream = message.getStream();
        String originalEventId = envelope == null ? message.getId() : envelope.id();
        if (!deadLetters.publish(commands, message, envelope, cause)) {
            deadLetterPublishFailures(stream).increment();
            redisErrors.increment();
            log.atError()
                    .addKeyValue("stream", stream)
                    .addKeyValue("message_id", message.getId())
                    .addKeyValue("original_event_id", originalEventId)
                    .addKeyValue("reason", reason.label())
                    .log("dead letter not published; entry left pending for the next reclaim pass");
            return;
        }
        deadLettered(stream, reason).increment();
        log.atWarn()
                .addKeyValue("stream", stream)
                .addKeyValue("message_id", message.getId())
                .addKeyValue("original_event_id", originalEventId)
                .addKeyValue("reason", reason.label())
                .log("entry dead-lettered");
        acknowledge(commands, message);
    }

    private void acknowledge(RedisCommands<String, String> commands, StreamMessage<String, String> message) {
        try {
            commands.xack(message.getStream(), properties.group(), message.getId());
        } catch (RedisException e) {
            redisErrors.increment();
            log.atError()
                    .addKeyValue("stream", message.getStream())
                    .addKeyValue("message_id", message.getId())
                    .setCause(e)
                    .log("ack ride assignment event failed; entry left pending");
        }
    }

    /** Whole seconds for the log; {@code -1} when the ID carries no timestamp. */
    private static long ageSeconds(String messageId, Instant now) {
        return StreamEntryAge.of(messageId, now).map(Duration::toSeconds).orElse(-1L);
    }

    private boolean pause(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
