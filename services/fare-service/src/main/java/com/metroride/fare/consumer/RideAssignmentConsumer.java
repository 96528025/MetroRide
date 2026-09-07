package com.metroride.fare.consumer;

import com.metroride.fare.FareServiceApplication;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.processing.ProcessedEventRecorder;
import com.metroride.fare.processing.ProcessedEventRecorder.Outcome;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;

/**
 * Consumer-group reader for {@code events.ride.assignments}, structured like {@code consume()} in
 * {@code services/dispatch-service/cmd/main.go}:
 *
 * <ol>
 *   <li>{@code XGROUP CREATE ... 0 MKSTREAM} at startup; an existing group is fine.</li>
 *   <li>Loop: {@code XREADGROUP GROUP g c COUNT n BLOCK t STREAMS stream >}.</li>
 *   <li>Per entry: decode the envelope, record it in one PostgreSQL transaction, then {@code XACK}.</li>
 * </ol>
 *
 * <p>The loop runs on its own single thread so it never competes with request handling, and
 * uses a dedicated Lettuce connection so the blocking read never stalls the shared connection the
 * readiness check and the rest of the application use.
 *
 * <p>Failure handling is intentionally minimal in this skeleton. An entry that cannot be decoded
 * or whose transaction fails is logged, counted, and left un-acknowledged in the group's pending
 * entry list; the loop moves on to the next entry.
 *
 * <p>TODO(pending-entry recovery): nothing re-reads that pending entry list. This service reads
 * only new entries ({@code >}), so an entry left pending by a crash between commit and XACK, a
 * transient PostgreSQL failure, or a malformed envelope is never retried. The Go consumers share
 * this gap (see docs/reliability.md, "Dispatch Service Restarts"). The fix belongs in this class:
 * on startup, and periodically, run {@code XAUTOCLAIM stream group consumer min-idle 0-0} and push
 * the claimed entries through {@link #handle}. Combine it with a delivery-count cap (available from
 * {@code XPENDING}) and a dead-letter publication to {@code events.dead_letter}, mirroring
 * {@code publishDeadLetter} in dispatch-service, so a poison entry cannot be claimed forever. It is
 * deferred because the retry and dead-letter policy should be chosen together with the first
 * business logic that can actually fail.
 */
@Component
public class RideAssignmentConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RideAssignmentConsumer.class);

    private final ConsumerProperties properties;
    private final LettuceConnectionFactory connectionFactory;
    private final EnvelopeCodec codec;
    private final ProcessedEventRecorder recorder;
    private final Duration shutdownTimeout;

    private final Counter recordedEvents;
    private final Counter duplicateEvents;
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
            MeterRegistry meterRegistry,
            @Value("${spring.lifecycle.timeout-per-shutdown-phase}") Duration shutdownTimeout) {
        this.properties = properties;
        this.connectionFactory = connectionFactory;
        this.codec = codec;
        this.recorder = recorder;
        this.shutdownTimeout = shutdownTimeout;

        // Registered eagerly so /metrics exposes every series at zero. The Go CounterVecs with the
        // same names only materialize a label set on its first increment.
        String service = FareServiceApplication.SERVICE_NAME;
        this.recordedEvents = meterRegistry.counter("metroride.fare.events.processed",
                "service", service, "stream", properties.stream(), "outcome", "recorded");
        this.duplicateEvents = meterRegistry.counter("metroride.fare.events.processed",
                "service", service, "stream", properties.stream(), "outcome", "duplicate");
        this.consumeErrors = meterRegistry.counter("metroride.stream.consume.errors",
                "service", service, "stream", properties.stream());
        this.redisErrors = meterRegistry.counter("metroride.dependency.errors",
                "service", service, "dependency", "redis");
        this.postgresErrors = meterRegistry.counter("metroride.dependency.errors",
                "service", service, "dependency", "postgres");
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
                .log("stream consumer started");
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // Let the loop finish the batch it already read: an entry that was delivered but neither
        // processed nor acknowledged would sit in the pending list with nobody to claim it.
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

        while (running.get()) {
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
                try {
                    handle(commands, message);
                } catch (RuntimeException e) {
                    // handle() deals with the expected failures itself. Anything else must not
                    // end this thread silently while isRunning() and /readyz still look healthy.
                    consumeErrors.increment();
                    log.atError().addKeyValue("message_id", message.getId()).setCause(e)
                            .log("unexpected failure handling ride assignment event; entry left pending");
                }
            }
        }
        log.info("stream consumer loop exited");
    }

    private void handle(RedisCommands<String, String> commands, StreamMessage<String, String> message) {
        Envelope envelope;
        try {
            envelope = codec.decode(message.getId(), message.getBody());
        } catch (EnvelopeDecodeException e) {
            consumeErrors.increment();
            log.atError().addKeyValue("message_id", message.getId()).setCause(e)
                    .log("decode ride assignment event failed; entry left pending");
            return;
        }

        Outcome outcome;
        try {
            outcome = recorder.record(properties.stream(), envelope);
        } catch (DataAccessException | TransactionException e) {
            postgresErrors.increment();
            log.atError()
                    .addKeyValue("message_id", message.getId())
                    .addKeyValue("event_id", envelope.id())
                    .addKeyValue("event_type", envelope.type())
                    .setCause(e)
                    .log("record event failed; entry left pending");
            return;
        }

        if (outcome == Outcome.RECORDED) {
            recordedEvents.increment();
        } else {
            duplicateEvents.increment();
        }
        log.atInfo()
                .addKeyValue("message_id", message.getId())
                .addKeyValue("event_id", envelope.id())
                .addKeyValue("event_type", envelope.type())
                .addKeyValue("source", envelope.source())
                .addKeyValue("ride_id", envelope.correlationId())
                .addKeyValue("outcome", outcome.name().toLowerCase())
                .log(outcome == Outcome.RECORDED ? "event recorded" : "duplicate event skipped");

        // Acknowledge only after the transaction above has committed. A failed ack is logged and
        // the entry stays pending; redelivery would hit the conflict clause and ack as a duplicate.
        try {
            commands.xack(properties.stream(), properties.group(), message.getId());
        } catch (RedisException e) {
            redisErrors.increment();
            log.atError().addKeyValue("message_id", message.getId()).setCause(e)
                    .log("ack ride assignment event failed");
        }
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
