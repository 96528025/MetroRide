package com.metroride.fare.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Payload of a {@code dead_lettered} envelope, mirroring {@code events.DeadLetter} in
 * {@code shared/pkg/events/events.go} field for field. {@code ride_id} is omitted when empty, as
 * the Go struct's {@code omitempty} tag does.
 *
 * @param originalEventId   ID of the envelope that failed, or the stream message ID when the entry
 *                          could not be decoded into an envelope at all
 * @param originalEventType type of the envelope that failed, or {@code decode_failed}
 * @param rideId            ride the event belonged to, when known
 * @param error             the failure, as text
 * @param service           the consumer that gave up, {@code fare-service}
 * @param failedAt          UTC timestamp in RFC 3339 form with fractional seconds
 */
public record DeadLetter(
        @JsonProperty("original_event_id") String originalEventId,
        @JsonProperty("original_event_type") String originalEventType,
        @JsonProperty("ride_id") @JsonInclude(JsonInclude.Include.NON_EMPTY) String rideId,
        @JsonProperty("error") String error,
        @JsonProperty("service") String service,
        @JsonProperty("failed_at") String failedAt) {
}
