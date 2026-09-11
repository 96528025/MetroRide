package com.metroride.fare.events;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * Java view of {@code events.Envelope} in {@code shared/pkg/events/events.go}. Field names are the
 * JSON names the Go side writes; {@code payload} stays raw (the Go side uses
 * {@code json.RawMessage}) so each event type decodes it on demand.
 *
 * @param id            stable event identifier; also the idempotency key in {@code fare.processed_events}
 * @param type          event type, for example {@code ride_assigned}
 * @param source        publishing service, for example {@code dispatch-service}
 * @param correlationId the ride ID for ride events
 * @param occurredAt    UTC timestamp written by the publisher with nanosecond precision; always an
 *                      RFC 3339 string on the wire, whatever the {@code ObjectMapper} defaults are,
 *                      because Go's {@code time.Time} does not accept a number
 * @param payload       event-specific JSON object, decoded with {@link EnvelopeCodec#decodePayload}
 */
public record Envelope(
        @JsonProperty("id") String id,
        @JsonProperty("type") String type,
        @JsonProperty("source") String source,
        @JsonProperty("correlation_id") String correlationId,
        @JsonProperty("occurred_at") @JsonFormat(shape = JsonFormat.Shape.STRING) Instant occurredAt,
        @JsonProperty("payload") JsonNode payload) {

    /** Stream and event-type names shared with the Go services. */
    public static final String STREAM_RIDE_ASSIGNMENTS = "events.ride.assignments";
    public static final String STREAM_RIDE_COMPLETIONS = "events.ride.completions";
    public static final String STREAM_DEAD_LETTER = "events.dead_letter";
    public static final String TYPE_RIDE_ASSIGNED = "ride_assigned";
    public static final String TYPE_RIDE_COMPLETED = "ride_completed";
    public static final String TYPE_DEAD_LETTERED = "dead_lettered";
    /** {@code original_event_type} of a dead letter whose entry was not a decodable envelope; see {@code publishDeadLetter} in dispatch-service. */
    public static final String TYPE_DECODE_FAILED = "decode_failed";
}
