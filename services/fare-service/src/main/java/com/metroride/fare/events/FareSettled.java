package com.metroride.fare.events;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Payload of a {@code fare_settled} envelope, mirroring {@code events.FareSettled} in
 * {@code shared/pkg/events/events.go} field for field. Published to {@code events.ride.fares}
 * in the transaction that writes the {@code hold_reversal} and the {@code settlement}.
 *
 * <p>Amounts and the share are decimal strings, as in the ledger endpoint, so no consumer turns
 * money into floating point by accident.
 *
 * @param rideId            the settled ride
 * @param riderId           from the {@code ride_completed} payload
 * @param driverId          from the {@code ride_completed} payload
 * @param assignmentId      from the {@code ride_completed} payload
 * @param settlementEventId ID of the {@code ride_completed} envelope that settled the ride, i.e.
 *                          the {@code source_event_id} of both journal entries
 * @param quote             the amount held and settled, two decimals
 * @param driverAmount      credited to {@code driver_payable}, two decimals
 * @param platformAmount    credited to {@code platform_revenue}, two decimals ({@code 0.00} when
 *                          the share left nothing)
 * @param driverShare       the configured {@code metroride.fare.driver-share}
 * @param settledAt         UTC, RFC 3339 with fractional seconds
 */
public record FareSettled(
        @JsonProperty("ride_id") String rideId,
        @JsonProperty("rider_id") String riderId,
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("assignment_id") String assignmentId,
        @JsonProperty("settlement_event_id") String settlementEventId,
        @JsonProperty("quote") String quote,
        @JsonProperty("driver_amount") String driverAmount,
        @JsonProperty("platform_amount") String platformAmount,
        @JsonProperty("driver_share") String driverShare,
        @JsonProperty("settled_at") String settledAt) {
}
