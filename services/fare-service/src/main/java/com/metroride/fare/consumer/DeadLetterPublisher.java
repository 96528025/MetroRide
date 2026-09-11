package com.metroride.fare.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.FareServiceApplication;
import com.metroride.fare.events.DeadLetter;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.events.RideAssigned;
import com.metroride.fare.events.RideCompleted;
import io.lettuce.core.RedisException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Writes a {@code dead_lettered} envelope to {@code events.dead_letter} in the exact shape
 * {@code publishDeadLetter} in {@code services/dispatch-service/cmd/main.go} writes it: a fresh
 * UUID as the envelope ID, {@code source} and {@code payload.service} naming this service,
 * {@code correlation_id} the ride ID when it is known, and an {@code events.DeadLetter} payload.
 * An entry that was never a decodable envelope is dead-lettered under its stream message ID with
 * type {@code decode_failed}, as the Go side does.
 *
 * <p>Publication is a single {@code XADD} on the caller's connection, so it runs on the consumer
 * thread between the failed transaction and the acknowledgement. It reports success or failure
 * instead of throwing: the caller acknowledges the original entry only on success and leaves it
 * pending otherwise. Nothing consumes {@code events.dead_letter} yet; the stream is the record.
 */
@Component
public class DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    private final ObjectMapper mapper;
    private final EnvelopeCodec codec;
    private final Clock clock;

    public DeadLetterPublisher(ObjectMapper mapper, EnvelopeCodec codec, Clock clock) {
        this.mapper = mapper;
        this.codec = codec;
        this.clock = clock;
    }

    /**
     * @param commands the consumer's own connection
     * @param message  the stream entry being given up on
     * @param original the entry decoded as an envelope, or {@code null} when decoding is what failed
     * @param cause    the failure; its message becomes the dead letter's {@code error}
     * @return whether Redis confirmed the {@code XADD}; on {@code false} nothing was published and
     *         the caller must leave {@code message} pending
     */
    public boolean publish(
            RedisCommands<String, String> commands,
            StreamMessage<String, String> message,
            Envelope original,
            Throwable cause) {
        Envelope deadLetter = envelope(message, original, cause);
        String json;
        try {
            json = mapper.writeValueAsString(deadLetter);
        } catch (JsonProcessingException e) {
            log.atError().addKeyValue("message_id", message.getId()).setCause(e)
                    .log("encode dead letter failed");
            return false;
        }
        try {
            String deadLetterId = commands.xadd(Envelope.STREAM_DEAD_LETTER, Map.of(EnvelopeCodec.EVENT_FIELD, json));
            log.atDebug()
                    .addKeyValue("message_id", message.getId())
                    .addKeyValue("dead_letter_id", deadLetterId)
                    .addKeyValue("dead_letter_event_id", deadLetter.id())
                    .log("dead letter published");
            return true;
        } catch (RedisException e) {
            log.atError().addKeyValue("message_id", message.getId()).setCause(e)
                    .log("publish dead letter failed");
            return false;
        }
    }

    /** The envelope that {@link #publish} writes; separate so its JSON is checked without Redis. */
    Envelope envelope(StreamMessage<String, String> message, Envelope original, Throwable cause) {
        String originalEventId;
        String originalEventType;
        String rideId;
        if (original == null) {
            originalEventId = message.getId();
            originalEventType = Envelope.TYPE_DECODE_FAILED;
            rideId = "";
        } else {
            originalEventId = original.id();
            originalEventType = original.type();
            rideId = rideIdOf(original);
        }
        DeadLetter payload = new DeadLetter(
                originalEventId,
                originalEventType,
                rideId,
                describe(cause),
                FareServiceApplication.SERVICE_NAME,
                DateTimeFormatter.ISO_INSTANT.format(clock.instant()));
        return new Envelope(
                UUID.randomUUID().toString(),
                Envelope.TYPE_DEAD_LETTERED,
                FareServiceApplication.SERVICE_NAME,
                rideId,
                clock.instant(),
                mapper.valueToTree(payload));
    }

    /**
     * The Go publisher prefers the payload's own {@code ride_id} over the correlation ID when the
     * payload decodes; for the ride events this service consumes they are the same value, but keep
     * the rule.
     */
    private String rideIdOf(Envelope original) {
        String fromPayload = null;
        try {
            if (Envelope.TYPE_RIDE_ASSIGNED.equals(original.type())) {
                fromPayload = codec.decodePayload(original, RideAssigned.class).rideId();
            } else if (Envelope.TYPE_RIDE_COMPLETED.equals(original.type())) {
                fromPayload = codec.decodePayload(original, RideCompleted.class).rideId();
            }
        } catch (EnvelopeDecodeException undecodablePayload) {
            // fall through to the correlation ID
        }
        if (fromPayload != null && !fromPayload.isBlank()) {
            return fromPayload;
        }
        return original.correlationId() == null ? "" : original.correlationId();
    }

    private static String describe(Throwable cause) {
        String message = cause.getMessage();
        return message == null || message.isBlank() ? cause.getClass().getName() : message;
    }
}
