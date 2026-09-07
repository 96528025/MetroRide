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
        // 60 * quote, exact: BigDecimal.valueOf(double) uses the double's shortest decimal
        // representation, so 1.8612 stays 1.8612 rather than its binary expansion.
        BigDecimal scaledBase = rates.baseFare().multiply(SECONDS_PER_MINUTE);
        BigDecimal scaledDistance = rates.perKm().multiply(BigDecimal.valueOf(distanceKm)).multiply(SECONDS_PER_MINUTE);
        BigDecimal scaledTime = rates.perMinute().multiply(BigDecimal.valueOf(etaSeconds));
        return Money.ofQuotient(scaledBase.add(scaledDistance).add(scaledTime), SECONDS_PER_MINUTE);
    }
}
