package com.metroride.fare.pricing;

import com.metroride.fare.ledger.Money;
import java.math.BigDecimal;

/**
 * Turns the distance and time of an assignment into a quoted fare:
 *
 * <pre>
 *   quote = base_fare + per_km * distance_km + per_minute * eta_seconds / 60
 * </pre>
 *
 * <p>A pure function of its inputs and the rate card: no Spring, no clock, no database. The sum is
 * computed exactly (the whole expression is scaled by 60 so there is no intermediate division) and
 * rounded to cents exactly once by {@link Money#ofQuotient}. Inputs that make no sense for a ride
 * are rejected rather than clamped, so a bad payload surfaces as a failure instead of a fare.
 */
public final class FareCalculator {

    private static final BigDecimal SECONDS_PER_MINUTE = BigDecimal.valueOf(60);

    private final FareProperties rates;

    public FareCalculator(FareProperties rates) {
        if (rates == null) {
            throw new IllegalArgumentException("fare rates are required");
        }
        this.rates = rates;
    }

    /**
     * @param distanceKm the assignment's {@code distance_km}; must be finite and not negative
     * @param etaSeconds the assignment's {@code eta_seconds}; must not be negative
     * @throws IllegalArgumentException for a negative, NaN or infinite input
     */
    public Money quote(double distanceKm, int etaSeconds) {
        if (Double.isNaN(distanceKm) || Double.isInfinite(distanceKm)) {
            throw new IllegalArgumentException("distance_km must be a finite number, got " + distanceKm);
        }
        if (distanceKm < 0) {
            throw new IllegalArgumentException("distance_km must not be negative, got " + distanceKm);
        }
        if (etaSeconds < 0) {
            throw new IllegalArgumentException("eta_seconds must not be negative, got " + etaSeconds);
        }
        return quote(BigDecimal.valueOf(distanceKm), BigDecimal.valueOf(etaSeconds));
    }

    /** Quotes the passenger's road distance and estimated duration, including fractional seconds. */
    public Money quote(BigDecimal tripDistanceKm, BigDecimal tripDurationSeconds) {
        if (tripDistanceKm == null || tripDistanceKm.signum() < 0) {
            throw new IllegalArgumentException("trip_distance_km must be present and not negative");
        }
        if (tripDurationSeconds == null || tripDurationSeconds.signum() < 0) {
            throw new IllegalArgumentException("trip_duration_seconds must be present and not negative");
        }
        BigDecimal scaledBase = rates.baseFare().multiply(SECONDS_PER_MINUTE);
        BigDecimal scaledDistance = rates.perKm().multiply(tripDistanceKm).multiply(SECONDS_PER_MINUTE);
        BigDecimal scaledTime = rates.perMinute().multiply(tripDurationSeconds);
        return Money.ofQuotient(scaledBase.add(scaledDistance).add(scaledTime), SECONDS_PER_MINUTE);
    }
}
