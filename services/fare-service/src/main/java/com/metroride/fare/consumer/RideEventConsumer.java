package com.metroride.fare.consumer;

import com.metroride.fare.FareServiceApplication;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.consumer.FailureHandler.Disposition;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.JournalKind;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.stereotype.Component;

/**
 * Consumer-group reader for {@code events.ride.assignments} and {@code events.ride.completions},
 * structured like {@code consume()} in {@code services/dispatch-service/cmd/main.go}, plus the
 * pending-entry recovery that the Go consumers do not have:
 *
 * <ol>
 *   <li>{@code XGROUP CREATE ... 0 MKSTREAM} on every configured stream at startup; an existing
 *       group is fine.</li>
 *   <li>Loop: every {@code reclaim-interval}, one
 *       {@code XAUTOCLAIM stream group consumer min-idle <cursor> COUNT n} per stream, where the
 *       cursor is what the previous pass of that stream returned ({@code 0-0} to start over), so
 *       a long pending list is walked in turn instead of its head being claimed again and again.
 *       Every claimed entry goes through {@link #handle} like a new one. Then one
 *       {@code XREADGROUP GROUP g c COUNT n BLOCK t STREAMS s1 s2 > >}.</li>
 *   <li>Per entry: decode the envelope and record it in one PostgreSQL transaction (for
 *       {@code ride_assigned}, quote the fare and append the hold; for {@code ride_completed},
 *       reverse the hold and settle), then {@code XACK}. Envelopes are dispatched by type, never
 *       by the stream they arrived on.</li>
 *   <li>Per failed entry: {@link FailureHandler} classifies the failure and disposes of the entry.
 *       Poison and quarantined entries are dead-lettered at once; retryable ones are left pending
 *       until their {@code max-deliveries}-th delivery fails, then dead-lettered too; fatal leaves
 *       the entry pending and halts this consumer ({@link ConsumerHalt}). Dead-lettering is
 *       {@code XADD} first and {@code XACK} only after Redis confirmed it, so a dead letter can be
 *       published twice; {@code original_event_id} is the key to deduplicate on.</li>
 * </ol>
 *
 * <p>Everything above runs on one thread over one dedicated Lettuce connection: the blocking read
 * never stalls the shared connection the readiness check uses, the reclaim pass never runs
 * concurrently with the read loop, and {@code ProcessedEventRecorder.record} keeps its single-writer
 * concurrency model within one instance. The reclaim pass is in this class rather than on a
 * scheduler for that reason.
 *
 * <p>Reclaim, acknowledgement and dead-lettering take the stream from the message itself
 * ({@link StreamMessage#getStream()}), the reclaim cursor is kept per stream, and every per-stream
 * metric is registered for each configured stream at startup.
 */
@Component
public class RideEventConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RideEventConsumer.class);

    /** {@code XAUTOCLAIM} cursor meaning "from the head of the pending entry list"; also what it returns once a scan is complete. */
    static final String RECLAIM_FROM_START = "0-0";

    /** Delivery count Redis assigns to an entry on its first {@code XREADGROUP} delivery. */
    private static final long FIRST_DELIVERY = 1;

    /** Redis scans at most this many times {@code COUNT} pending entries per {@code XAUTOCLAIM} (hard-coded server-side). */
    static final long XAUTOCLAIM_SCAN_FACTOR = 10;

    /** Delivery count reported when the {@code XPENDING} lookup for a reclaimed entry failed. */
    private static final long UNKNOWN_DELIVERY_COUNT = -1;

    private final ConsumerProperties properties;
    private final LettuceConnectionFactory connectionFactory;
    private final EnvelopeCodec codec;
    private final ProcessedEventRecorder recorder;
    private final FailureHandler failures;
    private final ConsumerHalt halt;
    private final Clock clock;
    private final MeterRegistry meterRegistry;
    private final Duration shutdownTimeout;

    private final Counter quoteHolds;
    private final Map<JournalKind, Counter> journalEntries = new EnumMap<>(JournalKind.class);
    private final Counter redisErrors;

    /**
     * Where the next {@code XAUTOCLAIM} of each stream starts. Written by the consumer thread only;
     * concurrent so a test can read it.
     */
    private final Map<String, String> reclaimCursors = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;
    private Future<?> loop;
    private StatefulRedisConnection<String, String> connection;

    public RideEventConsumer(
            ConsumerProperties properties,
            LettuceConnectionFactory connectionFactory,
            EnvelopeCodec codec,
            ProcessedEventRecorder recorder,
            FailureHandler failures,
            ConsumerHalt halt,
            Clock clock,
            MeterRegistry meterRegistry,
            @Value("${spring.lifecycle.timeout-per-shutdown-phase}") Duration shutdownTimeout) {
        this.properties = properties;
        this.connectionFactory = connectionFactory;
        this.codec = codec;
        this.recorder = recorder;
        this.failures = failures;
        this.halt = halt;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
        this.shutdownTimeout = shutdownTimeout;

        // Registered eagerly so /metrics exposes every series at zero. The Go CounterVecs with the
        // same names only materialize a label set on its first increment.
        String service = FareServiceApplication.SERVICE_NAME;
        this.quoteHolds = meterRegistry.counter("metroride.fare.quotes",
                "service", service, "kind", JournalKind.QUOTE_HOLD.code());
        for (JournalKind kind : JournalKind.values()) {
            journalEntries.put(kind, meterRegistry.counter("metroride.fare.journal.entries",
                    "service", service, "kind", kind.code()));
        }
        this.redisErrors = meterRegistry.counter("metroride.dependency.errors",
                "service", service, "dependency", "redis");
        // Per-stream series are looked up by the message's own stream when incremented;
        // registering them for every configured stream pins them at zero on /metrics.
        for (String stream : properties.streams()) {
            processedEvents(stream, "recorded");
            processedEvents(stream, "duplicate");
            consumeErrors(stream);
            reclaimedEntries(stream);
        }
    }

    private Counter processedEvents(String stream, String outcome) {
        return meterRegistry.counter("metroride.fare.events.processed",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream, "outcome", outcome);
    }

    private Counter consumeErrors(String stream) {
        return meterRegistry.counter("metroride.stream.consume.errors",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream);
    }

    private Counter reclaimedEntries(String stream) {
        return meterRegistry.counter("metroride.fare.events.reclaimed",
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
        if (properties.maxDeliveries() < 2) {
            running.set(false);
            throw new IllegalStateException("metroride.consumer.max-deliveries (" + properties.maxDeliveries()
                    + ") must be at least 2, or a retryable failure would be dead-lettered on its first delivery");
        }
        if (properties.streams() == null || properties.streams().isEmpty()
                || new HashSet<>(properties.streams()).size() != properties.streams().size()) {
            running.set(false);
            throw new IllegalStateException("metroride.consumer.streams must list at least one stream, each once, got "
                    + properties.streams());
        }

        // Standalone Redis only, which is all the Go services support as well.
        RedisClient client = (RedisClient) connectionFactory.getRequiredNativeClient();
        connection = client.connect();
        ensureConsumerGroups(connection.sync());

        executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "fare-stream-consumer");
            thread.setDaemon(false);
            return thread;
        });
        loop = executor.submit(this::runLoop);
        log.atInfo()
                .addKeyValue("streams", properties.streams())
                .addKeyValue("group", properties.group())
                .addKeyValue("consumer", properties.name())
                .addKeyValue("reclaim_interval", properties.reclaimInterval().toString())
                .addKeyValue("reclaim_min_idle", properties.reclaimMinIdle().toString())
                .addKeyValue("max_deliveries", properties.maxDeliveries())
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

    private void ensureConsumerGroups(RedisCommands<String, String> commands) {
        for (String stream : properties.streams()) {
            try {
                // "0" reads the stream from its beginning, as the Go ensureGroup does.
                commands.xgroupCreate(
                        XReadArgs.StreamOffset.from(stream, "0"),
                        properties.group(),
                        XGroupCreateArgs.Builder.mkstream(true));
            } catch (RedisBusyException alreadyExists) {
                // BUSYGROUP: the group is already there, which is the normal case after the first start.
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void runLoop() {
        RedisCommands<String, String> commands = connection.sync();
        Consumer<String> consumer = Consumer.from(properties.group(), properties.name());
        XReadArgs readArgs = XReadArgs.Builder
                .count(properties.batchSize())
                .block(properties.blockTimeout());
        // One XREADGROUP over every stream, each at ">" (entries never delivered to this group).
        XReadArgs.StreamOffset<String>[] newEntries = properties.streams().stream()
                .map(XReadArgs.StreamOffset::lastConsumed)
                .toArray(XReadArgs.StreamOffset[]::new);
        // The first pass runs before the first read, so whatever a previous instance left pending
        // is retried before any new entry is touched.
        Instant nextReclaim = clock.instant();

        while (running.get() && !halt.isHalted()) {
            if (!clock.instant().isBefore(nextReclaim)) {
                for (String stream : properties.streams()) {
                    reclaim(commands, consumer, stream);
                    if (halt.isHalted()) {
                        break;
                    }
                }
                nextReclaim = clock.instant().plus(properties.reclaimInterval());
                if (halt.isHalted()) {
                    break;
                }
            }
            List<StreamMessage<String, String>> messages;
            try {
                messages = commands.xreadgroup(consumer, readArgs, newEntries);
            } catch (RedisException e) {
                if (!running.get()) {
                    break;
                }
                // The one read covers every stream, so the failure is counted against each.
                for (String stream : properties.streams()) {
                    consumeErrors(stream).increment();
                }
                redisErrors.increment();
                log.atError().addKeyValue("streams", properties.streams()).setCause(e)
                        .log("read ride event streams failed");
                if (e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                    // A stream or group was deleted underneath us; recreate rather than spin.
                    try {
                        ensureConsumerGroups(commands);
                    } catch (RedisException recreate) {
                        log.error("recreate consumer group failed", recreate);
                    }
                }
                if (!pause(properties.errorBackoff())) {
                    break;
                }
                continue;
            }
            // The whole batch is handled even if stop() was called meanwhile; see stop(). A halt
            // is different: the rest of the batch stays pending for the next start to reclaim.
            for (StreamMessage<String, String> message : messages) {
                if (!handleGuarded(commands, message, FIRST_DELIVERY)) {
                    break;
                }
            }
        }
        if (halt.isHalted()) {
            log.atError().addKeyValue("reason", halt.reason().orElse(""))
                    .log("stream consumer halted; pending entries wait for a restart");
        }
        log.info("stream consumer loop exited");
    }

    /**
     * One {@code XAUTOCLAIM} over the group's pending entry list for {@code stream}, then the
     * claimed entries through the normal handler. The scan starts at the cursor the previous pass
     * returned and the cursor is saved whatever the pass claimed, even nothing: Redis scans at most
     * ten times {@code COUNT} entries per call and returns {@code 0-0} only when it reached the end
     * of the list, so restarting at the head each time would claim the same failing entries
     * forever and never reach the ones behind them. Claiming resets an entry's idle time, so an
     * entry that fails again waits another {@code reclaim-min-idle} before a pass can take it.
     */
    private void reclaim(RedisCommands<String, String> commands, Consumer<String> consumer, String stream) {
        String cursor = reclaimCursors.getOrDefault(stream, RECLAIM_FROM_START);
        List<StreamMessage<String, String>> claimed;
        try {
            var result = commands.xautoclaim(stream, XAutoClaimArgs.Builder
                    .xautoclaim(consumer, properties.reclaimMinIdle(), cursor)
                    .count(properties.batchSize()));
            reclaimCursors.put(stream, result.getId() == null ? RECLAIM_FROM_START : result.getId());
            claimed = result.getMessages();
        } catch (RedisException e) {
            if (!running.get()) {
                return;
            }
            redisErrors.increment();
            log.atError().addKeyValue("stream", stream).addKeyValue("cursor", cursor).setCause(e)
                    .log("reclaim pending entries failed");
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
            long deliveryCount = deliveryCounts.getOrDefault(message.getId(), UNKNOWN_DELIVERY_COUNT);
            reclaimedEntries(stream).increment();
            log.atInfo()
                    .addKeyValue("stream", stream)
                    .addKeyValue("message_id", message.getId())
                    .addKeyValue("age_seconds", FailureHandler.ageSeconds(message.getId(), now))
                    .addKeyValue("delivery_count", deliveryCount)
                    .log("pending entry reclaimed");
            if (!handleGuarded(commands, message, deliveryCount)) {
                return;
            }
        }
    }

    /** Where the next reclaim pass of {@code stream} starts; for tests. */
    String reclaimCursor(String stream) {
        return reclaimCursors.getOrDefault(stream, RECLAIM_FROM_START);
    }

    /**
     * {@code XAUTOCLAIM} returns the entries but not their delivery counts, so one {@code XPENDING}
     * over the claimed ID range fetches them. The count decides when a retryable entry has had its
     * {@code max-deliveries}, so the lookup must not miss a claimed entry: the range also holds this
     * consumer's entries that were skipped for being too fresh, and {@code XPENDING}'s count is an
     * upper bound on rows returned. Redis scans at most ten times {@code COUNT} pending entries per
     * {@code XAUTOCLAIM}, so every claimed entry lies in a window of that size and the range holds
     * at most that many entries; asking for that many is a sufficient upper bound. An entry whose count still could
     * not be fetched (the call failed) is reported as {@code -1} and is left pending on failure
     * rather than dead-lettered on a guess.
     */
    private Map<String, Long> deliveryCounts(
            RedisCommands<String, String> commands,
            Consumer<String> consumer,
            String stream,
            List<StreamMessage<String, String>> claimed) {
        Range<String> range = Range.create(claimed.get(0).getId(), claimed.get(claimed.size() - 1).getId());
        try {
            return commands.xpending(stream, consumer, range, Limit.from(XAUTOCLAIM_SCAN_FACTOR * properties.batchSize()))
                    .stream()
                    .collect(Collectors.toMap(PendingMessage::getId, PendingMessage::getRedeliveryCount, (a, b) -> a));
        } catch (RedisException e) {
            redisErrors.increment();
            log.atWarn().addKeyValue("stream", stream).setCause(e).log("read delivery counts of reclaimed entries failed");
            return Map.of();
        }
    }

    /** @return whether the loop may go on; {@code false} once the consumer has halted */
    private boolean handleGuarded(RedisCommands<String, String> commands, StreamMessage<String, String> message, long deliveryCount) {
        try {
            return handle(commands, message, deliveryCount);
        } catch (RuntimeException e) {
            // handle() classifies every failure of the work itself. Anything that escapes comes from
            // the failure handling (a bug), and must not end this thread silently while isRunning()
            // and /readyz still look healthy. The entry stays pending for the next reclaim pass.
            consumeErrors(message.getStream()).increment();
            log.atError().addKeyValue("stream", message.getStream()).addKeyValue("message_id", message.getId()).setCause(e)
                    .log("unexpected failure handling ride event; entry left pending");
            return true;
        }
    }

    private boolean handle(RedisCommands<String, String> commands, StreamMessage<String, String> message, long deliveryCount) {
        Envelope envelope = null;
        Result result;
        try {
            envelope = codec.decode(message.getId(), message.getBody());
            result = recorder.record(message.getStream(), envelope);
        } catch (RuntimeException e) {
            return failures.onFailure(commands, message, envelope, e, deliveryCount) != Disposition.HALT;
        }

        Outcome outcome = result.outcome();
        processedEvents(message.getStream(), outcome.name().toLowerCase()).increment();
        // Counted here, after the commit, so a rolled-back transaction never counts as written.
        for (JournalEntry written : result.entries()) {
            journalEntries.get(written.kind()).increment();
            if (written.kind() == JournalKind.QUOTE_HOLD) {
                quoteHolds.increment();
            }
        }
        var entry = log.atInfo()
                .addKeyValue("stream", message.getStream())
                .addKeyValue("message_id", message.getId())
                .addKeyValue("event_id", envelope.id())
                .addKeyValue("event_type", envelope.type())
                .addKeyValue("source", envelope.source())
                .addKeyValue("ride_id", envelope.correlationId())
                .addKeyValue("outcome", outcome.name().toLowerCase());
        if (!result.entries().isEmpty()) {
            // The first posting of the first entry is the quoted amount X for every kind written.
            entry = entry.addKeyValue("journal_kinds",
                            result.entries().stream().map(written -> written.kind().code()).collect(Collectors.joining(",")))
                    .addKeyValue("amount", result.entries().get(0).postings().get(0).amount().toString().replace("-", ""));
        }
        entry.log(outcome == Outcome.RECORDED ? "event recorded" : "duplicate event skipped");

        // Acknowledge only after the transaction above has committed. A failed ack is logged and
        // the entry stays pending; its reclaimed delivery hits the conflict clause and acks as a
        // duplicate.
        failures.acknowledge(commands, message);
        return true;
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
