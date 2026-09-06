package com.metroride.fare.events;

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
 * @param occurredAt    UTC timestamp written by the publisher with nanosecond precision
 * @param payload       event-specific JSON object, decoded with {@link EnvelopeCodec#decodePayload}
 */
public record Envelope(
        @JsonProperty("id") String id,
        @JsonProperty("type") String type,
        @JsonProperty("source") String source,
        @JsonProperty("correlation_id") String correlationId,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payload") JsonNode payload) {

    /** Stream and event-type names shared with the Go services. */
    public static final String STREAM_RIDE_ASSIGNMENTS = "events.ride.assignments";
    public static final String TYPE_RIDE_ASSIGNED = "ride_assigned";
    public static final String TYPE_RIDE_COMPLETED = "ride_completed";
}
