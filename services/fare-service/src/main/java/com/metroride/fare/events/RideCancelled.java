package com.metroride.fare.events;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RideCancelled(
        @JsonProperty("ride_id") String rideId,
        @JsonProperty("rider_id") String riderId,
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("assignment_id") String assignmentId,
        @JsonProperty("cancelled_at") String cancelledAt) {
}
