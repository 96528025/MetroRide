package com.metroride.fare.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class MoneyTest {

    @Test
    void ofRoundsHalfUpToCents() {
        assertThat(Money.of(new BigDecimal("2.505"))).isEqualTo(Money.of("2.51"));
        assertThat(Money.of(new BigDecimal("2.504999"))).isEqualTo(Money.of("2.50"));
        assertThat(Money.of(new BigDecimal("-2.505"))).isEqualTo(Money.of("-2.51"));
        assertThat(Money.of("3")).hasToString("3.00");
    }

    @Test
    void ofQuotientRoundsTheExactFractionOnce() {
        assertThat(Money.ofQuotient(BigDecimal.ONE, BigDecimal.valueOf(3))).hasToString("0.33");
        assertThat(Money.ofQuotient(BigDecimal.valueOf(2), BigDecimal.valueOf(3))).hasToString("0.67");
        assertThatThrownBy(() -> Money.ofQuotient(BigDecimal.ONE, BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** The driver's share: the exact product is rounded once, half up, and only here. */
    @Test
    void timesRoundsTheExactProductOnce() {
        assertThat(Money.of("5.85").times(new BigDecimal("0.80"))).hasToString("4.68");
        // 0.008 rounds up to a cent; 0.004 rounds to nothing.
        assertThat(Money.of("0.01").times(new BigDecimal("0.80"))).hasToString("0.01");
        assertThat(Money.of("0.01").times(new BigDecimal("0.40"))).hasToString("0.00");
        // Exact .5 boundary, half up.
        assertThat(Money.of("1.25").times(new BigDecimal("0.5"))).hasToString("0.63");
        assertThat(Money.of("5.85").times(BigDecimal.ZERO)).isEqualTo(Money.ZERO);
        assertThat(Money.of("5.85").times(BigDecimal.ONE)).isEqualTo(Money.of("5.85"));
        assertThatThrownBy(() -> Money.of("1.00").times(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorAcceptsOnlyScaleTwo() {
        assertThatThrownBy(() -> new Money(new BigDecimal("1.5")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scale");
        assertThatThrownBy(() -> new Money(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAmountsThatDoNotFitNumeric12_2() {
        assertThat(Money.of("9999999999.99")).hasToString("9999999999.99");
        assertThatThrownBy(() -> Money.of("10000000000.00"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds numeric(12, 2)");
    }

    @Test
    void arithmeticKeepsTheScale() {
        Money sum = Money.of("5.85").plus(Money.of("5.85").negate());
        assertThat(sum).isEqualTo(Money.ZERO);
        assertThat(sum.isZero()).isTrue();
        assertThat(Money.of("-0.01").isNegative()).isTrue();
        assertThat(Money.of("1.10")).isEqualTo(Money.of("1.1"));
    }
}
