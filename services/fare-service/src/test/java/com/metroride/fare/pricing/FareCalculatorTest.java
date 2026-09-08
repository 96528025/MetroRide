package com.metroride.fare.pricing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.metroride.fare.ledger.Money;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class FareCalculatorTest {

    /** The rate card from application.yml. */
    private static final FareProperties RATES = rates("2.50", "1.20", "0.30", "0.80");

    private final FareCalculator calculator = new FareCalculator(RATES);

    @Test
    void zeroDistanceAndZeroTimeIsTheBaseFare() {
        assertThat(calculator.quote(0, 0)).isEqualTo(Money.of("2.50"));
    }

    @Test
    void timeOnlyAddsThePerMinuteRate() {
        // 2.50 + 0.30 * 600 / 60 = 5.50
        assertThat(calculator.quote(0, 600)).isEqualTo(Money.of("5.50"));
    }

    @Test
    void distanceOnlyAddsThePerKmRate() {
        // 2.50 + 1.20 * 2.5 = 5.50
        assertThat(calculator.quote(2.5, 0)).isEqualTo(Money.of("5.50"));
    }

    @Test
    void quotesTheAssignmentFromTheIntegrationTest() {
        // 2.50 + 1.20 * 1.8612 + 0.30 * 223 / 60 = 2.50 + 2.23344 + 1.115 = 5.84844 -> 5.85
        assertThat(calculator.quote(1.8612, 223)).isEqualTo(Money.of("5.85"));
    }

    @Test
    void veryLongDistanceStaysExact() {
        // 2.50 + 1.20 * 40075.017 = 48092.5204 -> 48092.52
        assertThat(calculator.quote(40075.017, 0)).isEqualTo(Money.of("48092.52"));
    }

    @Test
    void distanceBeyondTheLedgerColumnIsRejectedNotTruncated() {
        // 1.20 * 1e10 km would need 13 integer digits; numeric(12, 2) holds at most ten.
        assertThatThrownBy(() -> calculator.quote(1e10, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds numeric(12, 2)");
    }

    @Test
    void roundsHalfUpOnTheExactValueNotOnAnIntermediate() {
        FareCalculator perKmOnly = new FareCalculator(rates("0", "1.00", "0", "0"));
        // exactly 2.505: half up gives 2.51, half even would give 2.50
        assertThat(perKmOnly.quote(2.505, 0)).isEqualTo(Money.of("2.51"));
        assertThat(perKmOnly.quote(2.5049, 0)).isEqualTo(Money.of("2.50"));

        FareCalculator perMinuteOnly = new FareCalculator(rates("0", "0", "0.30", "0"));
        // 0.30 * 225 / 60 = 1.125 exactly: half up gives 1.13, half even would give 1.12
        assertThat(perMinuteOnly.quote(0, 225)).isEqualTo(Money.of("1.13"));
        // 0.30 * 1 / 60 = 0.005 exactly: a non-terminating division if done per minute first
        assertThat(perMinuteOnly.quote(0, 1)).isEqualTo(Money.of("0.01"));
    }

    @Test
    void rejectsNegativeDistance() {
        assertThatThrownBy(() -> calculator.quote(-0.1, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distance_km");
    }

    @Test
    void rejectsNegativeTime() {
        assertThatThrownBy(() -> calculator.quote(1.0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eta_seconds");
    }

    @Test
    void rejectsNonFiniteDistance() {
        assertThatThrownBy(() -> calculator.quote(Double.NaN, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.quote(Double.POSITIVE_INFINITY, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rateCardRejectsNegativeRatesAndAShareAboveOne() {
        assertThatThrownBy(() -> rates("-1", "1", "1", "0.5")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> rates("1", "1", "1", "1.5")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FareProperties(null, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static FareProperties rates(String base, String perKm, String perMinute, String driverShare) {
        return new FareProperties(new BigDecimal(base), new BigDecimal(perKm), new BigDecimal(perMinute), new BigDecimal(driverShare));
    }
}
