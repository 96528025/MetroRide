package com.metroride.fare.consumer;

import com.metroride.fare.FareServiceApplication;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.pricing.FareQuoteException;
import io.lettuce.core.RedisException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;

/**
 * What happens to a stream entry after {@code handle()} has finished with it: the acknowledgement
 * on success, and on failure the choice between leaving it pending, dead-lettering it, or halting
 * the consumer. Both the success acknowledgement and the post-dead-letter acknowledgement go
 * through {@link #acknowledge}, so there is one {@code XACK} in the service.
 *
 * <p>Separate from the consumer so the guarantees that matter most can be tested without Redis:
 * no {@code XACK} unless the dead-letter {@code XADD} was confirmed, an {@code XACK} that fails
 * leaves the entry pending, and a fatal failure neither acknowledges nor dead-letters.
 */
@Component
public class FailureHandler {

    private static final Logger log = LoggerFactory.getLogger(FailureHandler.class);

    /** Where the entry ended up. */
    public enum Disposition {
        /** Still in the pending entry list; a later reclaim pass delivers it again. */
        LEFT_PENDING,
        /** On {@code events.dead_letter} and acknowledged. */
        DEAD_LETTERED,
        /** Still pending, and the consumer must stop: see {@link FailureClass#FATAL}. */
        HALT
    }

    private final ConsumerProperties properties;
    private final DeadLetterPublisher deadLetters;
    private final ConsumerHalt halt;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    private final Map<FareQuoteException.Reason, Counter> quoteFailures = new EnumMap<>(FareQuoteException.Reason.class);
    private final Counter consumeErrors;
    private final Counter redisErrors;
    private final Counter postgresErrors;

    public FailureHandler(
            ConsumerProperties properties,
            DeadLetterPublisher deadLetters,
            ConsumerHalt halt,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.properties = properties;
        this.deadLetters = deadLetters;
        this.halt = halt;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
        String service = FareServiceApplication.SERVICE_NAME;
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
        // Per-stream series are looked up by the message's own stream when incremented;
        // registering them for the configured stream only pins them at zero on /metrics.
        for (DeadLetterReason reason : DeadLetterReason.values()) {
            deadLettered(properties.stream(), reason);
        }
        deadLetterPublishFailures(properties.stream());
    }

    /**
     * The transaction has rolled back (or never started), so nothing of this entry is recorded.
     *
     * @param envelope      the entry decoded, or {@code null} when decoding is what failed
     * @param deliveryCount how many times Redis has delivered this entry, {@code 1} for a fresh
     *                      read; a value below {@code 1} means the count could not be fetched and
     *                      never exhausts the budget
     */
    public Disposition onFailure(
            RedisCommands<String, String> commands,
            StreamMessage<String, String> message,
            Envelope envelope,
            RuntimeException failure,
            long deliveryCount) {
        count(failure);
        FailureClass failureClass = FailureClass.of(failure);
        LoggingEventBuilder entry = log.atError()
                .addKeyValue("stream", message.getStream())
                .addKeyValue("message_id", message.getId())
                .addKeyValue("failure_class", failureClass.name().toLowerCase())
                .addKeyValue("delivery_count", deliveryCount)
                .addKeyValue("age_seconds", ageSeconds(message.getId(), clock.instant()))
                .setCause(failure);
        if (envelope != null) {
            entry = entry.addKeyValue("event_id", envelope.id())
                    .addKeyValue("event_type", envelope.type())
                    .addKeyValue("ride_id", envelope.correlationId());
        }
        return switch (failureClass) {
            case POISON -> {
                entry.log("handle event failed; dead-lettering poison entry");
                yield deadLetter(commands, message, envelope, failure, DeadLetterReason.POISON);
            }
            case RETRYABLE -> {
                entry = entry.addKeyValue("max_deliveries", properties.maxDeliveries());
                if (deliveryCount >= properties.maxDeliveries()) {
                    entry.log("handle event failed; max deliveries reached, dead-lettering entry");
                    yield deadLetter(commands, message, envelope, failure, DeadLetterReason.RETRY_BUDGET_EXHAUSTED);
                }
                entry.log("handle event failed; entry left pending for the next reclaim pass");
                yield Disposition.LEFT_PENDING;
            }
            case FATAL -> {
                entry.log("handle event failed; fatal failure, halting consumer with the entry left pending");
                halt.halt(haltReason(message, failure));
                yield Disposition.HALT;
            }
        };
    }

    /**
     * {@code XACK} after a committed transaction or a confirmed dead letter. A failed ack is
     * logged and counted, and the entry stays pending: its next delivery lands on the conflict
     * clause (or dead-letters it again) and acks then.
     *
     * @return whether Redis confirmed the acknowledgement
     */
    public boolean acknowledge(RedisCommands<String, String> commands, StreamMessage<String, String> message) {
        try {
            commands.xack(message.getStream(), properties.group(), message.getId());
            return true;
        } catch (RedisException e) {
            redisErrors.increment();
            log.atError()
                    .addKeyValue("stream", message.getStream())
                    .addKeyValue("message_id", message.getId())
                    .setCause(e)
                    .log("ack event failed; entry left pending");
            return false;
        }
    }

    /**
     * Dead letter first, acknowledge second. If the {@code XADD} is not confirmed the entry stays
     * pending and is dead-lettered again on a later pass; if the {@code XACK} fails after a
     * confirmed {@code XADD} the same happens and the dead letter is duplicated. Both are preferable
     * to acknowledging an entry whose dead letter never landed.
     */
    private Disposition deadLetter(
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
            return Disposition.LEFT_PENDING;
        }
        deadLettered(stream, reason).increment();
        log.atWarn()
                .addKeyValue("stream", stream)
                .addKeyValue("message_id", message.getId())
                .addKeyValue("original_event_id", originalEventId)
                .addKeyValue("reason", reason.label())
                .log("entry dead-lettered");
        return acknowledge(commands, message) ? Disposition.DEAD_LETTERED : Disposition.LEFT_PENDING;
    }

    /** The exception and, when it wraps one, the root cause: Spring's translations rarely repeat the driver's message. */
    private static String haltReason(StreamMessage<String, String> message, RuntimeException failure) {
        String reason = "fatal failure handling " + message.getStream() + "/" + message.getId() + ": " + failure;
        Throwable root = NestedExceptionUtils.getMostSpecificCause(failure);
        return root == failure ? reason : reason + " (cause: " + root + ")";
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

    private Counter deadLettered(String stream, DeadLetterReason reason) {
        return meterRegistry.counter("metroride.fare.dead_letters",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream, "reason", reason.label());
    }

    private Counter deadLetterPublishFailures(String stream) {
        return meterRegistry.counter("metroride.fare.dead_letter.publish.failures",
                "service", FareServiceApplication.SERVICE_NAME, "stream", stream);
    }

    /** Whole seconds since the entry's {@code XADD}, for the log only; {@code -1} when the ID carries no timestamp. */
    static long ageSeconds(String messageId, Instant now) {
        return StreamEntryAge.of(messageId, now).map(Duration::toSeconds).orElse(-1L);
    }
}
