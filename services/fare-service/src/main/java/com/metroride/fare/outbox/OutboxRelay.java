package com.metroride.fare.outbox;

import com.metroride.fare.FareServiceApplication;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.outbox.OutboxRepository.PendingEvent;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Publishes {@code fare.event_outbox} rows to their Redis Streams, the counterpart of
 * {@code Relay.Run} in {@code shared/pkg/outbox/outbox.go}, with the same guarantees:
 *
 * <ul>
 *   <li>Every {@code poll-interval}, one pass: in one transaction take up to {@code batch-size}
 *       rows with {@code for update skip locked}, {@code XADD} each, mark the successes
 *       {@code published_at} and give each failure {@code publish_attempts + 1} and a capped
 *       exponential {@code next_attempt_at}; commit the batch.</li>
 *   <li>A failed destination does not block the rest of the batch, and eligible rows are ordered
 *       by retry time, so retries and new rows both keep moving.</li>
 *   <li>At-least-once. If the process dies after Redis accepted an entry and before the
 *       transaction that records {@code published_at} commits, the row is published again with
 *       the same envelope ID. A failure to mark a row published aborts the transaction for the
 *       same reason, so the batch's earlier successes are published again on the next pass.
 *       Consumers deduplicate on the envelope ID, as this service does for its own streams.</li>
 * </ul>
 *
 * <p>What is deliberately not mirrored: the Go relay bounds each statement, the commit and each
 * publish with its own 2s context and nothing else; here each statement carries a 2s query
 * timeout, each {@code XADD} the 2s Redis command timeout, the commit and rollback (which no query
 * timeout covers) the datasource's 5s socket timeout, and the transaction has no overall budget.
 * Putting the batch under the consumer's transaction timeout would cancel the update that records
 * a slow publish's failure, roll back the backoff with it, and retry the row every poll.
 *
 * <p>Own thread and own Lettuce connection, so publishing never competes with the consumer's
 * blocking read or with the readiness check. Shutdown lets the current pass finish and exits;
 * rows still unpublished are published by the next start, as with the Go relay. The relay is not
 * a readiness dependency: with Redis away, settlements still commit and their rows wait here.
 */
@Component
public class OutboxRelay implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxProperties properties;
    private final OutboxRepository repository;
    private final TransactionOperations transaction;
    private final LettuceConnectionFactory connectionFactory;
    private final MeterRegistry meterRegistry;
    private final Duration shutdownTimeout;
    private final AtomicLong unpublished = new AtomicLong();
    private final Counter passFailures;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;
    private Future<?> loop;
    private StatefulRedisConnection<String, String> connection;

    public OutboxRelay(
            OutboxProperties properties,
            OutboxRepository repository,
            TransactionOperations outboxTransaction,
            LettuceConnectionFactory connectionFactory,
            MeterRegistry meterRegistry,
            @Value("${spring.lifecycle.timeout-per-shutdown-phase}") Duration shutdownTimeout) {
        this.properties = properties;
        this.repository = repository;
        this.transaction = outboxTransaction;
        this.connectionFactory = connectionFactory;
        this.meterRegistry = meterRegistry;
        this.shutdownTimeout = shutdownTimeout;
        // The Go names, pre-registered for the one stream this service publishes so /metrics
        // shows them at zero; incremented by the row's own stream.
        published(Envelope.STREAM_RIDE_FARES);
        publishFailures(Envelope.STREAM_RIDE_FARES);
        Gauge.builder("metroride.fare.outbox.unpublished", unpublished, AtomicLong::get)
                .tag("service", FareServiceApplication.SERVICE_NAME)
                .register(meterRegistry);
        this.passFailures = meterRegistry.counter("metroride.fare.outbox.pass.failures",
                "service", FareServiceApplication.SERVICE_NAME);
    }

    private Counter published(String stream) {
        return meterRegistry.counter("metroride.outbox.events.published",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream);
    }

    private Counter publishFailures(String stream) {
        return meterRegistry.counter("metroride.outbox.publish.failures",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream);
    }

    // ---- lifecycle -------------------------------------------------------------------------

    @Override
    public void start() {
        if (!properties.enabled()) {
            log.warn("outbox relay disabled by metroride.outbox.enabled; rows are enqueued but not published by this instance");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        RedisClient client = (RedisClient) connectionFactory.getRequiredNativeClient();
        connection = client.connect();
        executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "fare-outbox-relay");
            thread.setDaemon(false);
            return thread;
        });
        loop = executor.submit(this::runLoop);
        log.atInfo()
                .addKeyValue("poll_interval", properties.pollInterval().toString())
                .addKeyValue("batch_size", properties.batchSize())
                .addKeyValue("max_retry_backoff", properties.maxRetryBackoff().toString())
                .log("outbox relay started");
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        executor.shutdown();
        try {
            loop.get(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("outbox relay did not stop within {}; abandoning the loop", shutdownTimeout);
            loop.cancel(true);
        } catch (ExecutionException e) {
            log.error("outbox relay loop terminated with an error", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            connection.close();
            log.info("outbox relay stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    // ---- publication -----------------------------------------------------------------------

    private void runLoop() {
        RedisCommands<String, String> commands = connection.sync();
        while (running.get()) {
            try {
                publishPending(commands);
            } catch (DataAccessException | TransactionException e) {
                // The batch's transaction failed as a whole (a statement timed out, the connection
                // dropped or hit its socket timeout, or a published row could not be marked):
                // nothing was committed, every row of it is eligible again on the next pass. A
                // commit that was cut off by the socket timeout may still have completed on the
                // server; then the rows are marked and are not published again.
                passFailures.increment();
                log.atError().setCause(e).log("outbox relay pass failed; batch will be retried");
            } catch (RuntimeException e) {
                passFailures.increment();
                log.atError().setCause(e).log("outbox relay pass failed unexpectedly; batch will be retried");
            }
            try {
                unpublished.set(repository.countUnpublished());
            } catch (DataAccessException e) {
                log.atWarn().setCause(e).log("count unpublished outbox rows failed");
            }
            try {
                Thread.sleep(properties.pollInterval().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("outbox relay loop exited");
    }

    /** One pass; package-private so the batch semantics are unit-tested against a mocked Redis. */
    Summary publishPending(RedisCommands<String, String> commands) {
        Summary summary = transaction.execute(status -> {
            List<PendingEvent> batch = repository.lockPending(properties.batchSize());
            List<PendingEvent> ok = new ArrayList<>();
            List<PendingEvent> failed = new ArrayList<>();
            for (PendingEvent event : batch) {
                try {
                    commands.xadd(event.stream(), Map.of(EnvelopeCodec.EVENT_FIELD, event.envelopeJson()));
                } catch (RedisException e) {
                    double delaySeconds = RetryBackoff.delay(event.publishAttempts(),
                            properties.pollInterval(), properties.maxRetryBackoff()).toMillis() / 1000.0;
                    repository.markFailed(event, delaySeconds, describe(e));
                    failed.add(event);
                    log.atWarn()
                            .addKeyValue("event_id", event.id())
                            .addKeyValue("stream", event.stream())
                            .addKeyValue("publish_attempts", event.publishAttempts() + 1)
                            .addKeyValue("retry_in_seconds", delaySeconds)
                            .setCause(e)
                            .log("publish outbox event failed; scheduled for retry");
                    continue;
                }
                // Redis has the entry. If this update fails the transaction aborts, the row stays
                // unpublished, and the next pass publishes it again: the at-least-once window.
                repository.markPublished(event);
                ok.add(event);
            }
            return new Summary(ok, failed);
        });
        // Counted after the commit, like the Go relay, so an aborted batch counts nothing.
        for (PendingEvent event : summary.published()) {
            published(event.stream()).increment();
            log.atInfo().addKeyValue("event_id", event.id()).addKeyValue("stream", event.stream())
                    .log("outbox event published");
        }
        for (PendingEvent event : summary.failed()) {
            publishFailures(event.stream()).increment();
        }
        return summary;
    }

    private static String describe(RedisException e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getName() : message;
    }

    /** What one pass committed. */
    record Summary(List<PendingEvent> published, List<PendingEvent> failed) {
    }
}
