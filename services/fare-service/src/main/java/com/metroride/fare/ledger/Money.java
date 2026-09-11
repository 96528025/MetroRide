package com.metroride.fare.ledger;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * An amount of money with exactly two decimal places, the shape of the {@code numeric(12, 2)}
 * ledger columns.
 *
 * <p>This is the only class that knows the scale and the rounding mode. Every other part of the
 * service does its arithmetic in exact {@link BigDecimal} terms and hands the result here to be
 * rounded once, so two code paths can never round the same figure differently.
 *
 * @param amount the amount, always at scale {@value #SCALE}
 */
public record Money(BigDecimal amount) implements Comparable<Money> {

    /** Decimal places, matching {@code numeric(12, 2)}. */
    public static final int SCALE = 2;

    /** Total digits, matching {@code numeric(12, 2)}: the largest amount is 9,999,999,999.99. */
    public static final int PRECISION = 12;

    private static final RoundingMode ROUNDING = RoundingMode.HALF_UP;
    private static final BigDecimal MAX_ABS = BigDecimal.TEN.pow(PRECISION - SCALE).subtract(BigDecimal.ONE.movePointLeft(SCALE));

    public static final Money ZERO = new Money(BigDecimal.ZERO.setScale(SCALE));

    public Money {
        if (amount == null) {
            throw new IllegalArgumentException("amount is required");
        }
        if (amount.scale() != SCALE) {
            throw new IllegalArgumentException("amount " + amount + " must have scale " + SCALE + "; use Money.of");
        }
        if (amount.abs().compareTo(MAX_ABS) > 0) {
            throw new IllegalArgumentException("amount " + amount + " exceeds numeric(" + PRECISION + ", " + SCALE + ")");
        }
    }

    /** Rounds {@code exact} to cents, half up. */
    public static Money of(BigDecimal exact) {
        if (exact == null) {
            throw new IllegalArgumentException("amount is required");
        }
        return new Money(exact.setScale(SCALE, ROUNDING));
    }

    /** Parses a literal such as {@code "2.50"}; the value is rounded like {@link #of(BigDecimal)}. */
    public static Money of(String literal) {
        return of(new BigDecimal(literal));
    }

    /**
     * Rounds the exact quotient {@code numerator / denominator} to cents, half up. Callers whose
     * exact value is a fraction that does not terminate (a per-minute rate applied to seconds, for
     * example) pass the fraction here instead of dividing themselves, so the single rounding step
     * happens on the exact value and not on an already-rounded intermediate.
     */
    public static Money ofQuotient(BigDecimal numerator, BigDecimal denominator) {
        if (numerator == null || denominator == null) {
            throw new IllegalArgumentException("numerator and denominator are required");
        }
        if (denominator.signum() == 0) {
            throw new IllegalArgumentException("denominator must not be zero");
        }
        return new Money(numerator.divide(denominator, SCALE, ROUNDING));
    }

    /**
     * This amount times an exact factor, rounded to cents once, half up. Used for the driver's
     * share of a settled fare: the exact product is rounded here and nowhere else, and the
     * platform's share is what remains after that rounding, so the two always add up to the fare.
     */
    public Money times(BigDecimal factor) {
        if (factor == null) {
            throw new IllegalArgumentException("factor is required");
        }
        return of(amount.multiply(factor));
    }

    public Money plus(Money other) {
        return new Money(amount.add(other.amount));
    }

    public Money negate() {
        return new Money(amount.negate());
    }

    public boolean isZero() {
        return amount.signum() == 0;
    }

    public boolean isNegative() {
        return amount.signum() < 0;
    }

    @Override
    public int compareTo(Money other) {
        return amount.compareTo(other.amount);
    }

    /** The plain decimal form, for example {@code 12.30}; what the ledger API returns. */
    @Override
    public String toString() {
        return amount.toPlainString();
    }
}
