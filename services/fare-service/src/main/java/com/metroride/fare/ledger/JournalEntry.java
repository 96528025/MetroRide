package com.metroride.fare.ledger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * A balanced set of postings on one ride, produced by one consumed event.
 *
 * <p>The double-entry invariant lives here: the constructor rejects an empty posting list and any
 * list whose amounts do not sum to zero, and it copies the list so it cannot change afterwards. An
 * unbalanced entry therefore cannot exist in memory, let alone reach the database. The tables carry
 * no trigger for the same rule; see the README for the reasoning.
 *
 * @param rideId        the ride whose ledger this entry belongs to
 * @param kind          why the entry exists
 * @param sourceEventId the envelope ID that produced it; unique together with {@code kind}
 * @param postings      at least one posting, summing to zero
 */
public record JournalEntry(String rideId, JournalKind kind, String sourceEventId, List<Posting> postings) {

    public JournalEntry {
        if (rideId == null || rideId.isBlank()) {
            throw new IllegalArgumentException("journal entry needs a ride id");
        }
        if (kind == null) {
            throw new IllegalArgumentException("journal entry needs a kind");
        }
        if (sourceEventId == null || sourceEventId.isBlank()) {
            throw new IllegalArgumentException("journal entry needs a source event id");
        }
        if (postings == null || postings.isEmpty()) {
            throw new IllegalArgumentException("journal entry needs at least one posting");
        }
        postings = List.copyOf(postings);
        Money total = Money.ZERO;
        for (Posting posting : postings) {
            total = total.plus(posting.amount());
        }
        if (!total.isZero()) {
            throw new IllegalArgumentException("journal entry postings sum to " + total + ", not zero");
        }
    }

    /**
     * The entry written when a ride is assigned: the quoted fare is debited to the rider's
     * receivable and credited to the fare hold. Nothing is earned yet.
     */
    public static JournalEntry quoteHold(String rideId, String sourceEventId, Money quote) {
        return new JournalEntry(rideId, JournalKind.QUOTE_HOLD, sourceEventId, List.of(
                Posting.debit(Account.RIDER_RECEIVABLE, quote),
                Posting.credit(Account.FARE_HOLD, quote)));
    }

    /**
     * The first of the two entries written when a ride is completed: the {@link #quoteHold} of the
     * same amount, posting for posting with the opposite sign, so the rider's receivable and the
     * hold both return to zero before the settlement is posted.
     */
    public static JournalEntry holdReversal(String rideId, String sourceEventId, Money quote) {
        return new JournalEntry(rideId, JournalKind.HOLD_REVERSAL, sourceEventId, List.of(
                Posting.credit(Account.RIDER_RECEIVABLE, quote),
                Posting.debit(Account.FARE_HOLD, quote)));
    }

    /**
     * The second entry written when a ride is completed: the quoted fare {@code X} is debited to
     * the rider's receivable again and split between the driver and the platform. The driver's
     * share {@code D} is {@code X} times {@code driverShare}, rounded to cents once by
     * {@link Money#times}; the platform's share is the remainder {@code X - D}, so the entry
     * balances by construction and no cent is created or lost by rounding.
     *
     * <p>A posting whose amount is zero is omitted rather than written, because a zero posting is
     * refused by {@link Posting}:
     *
     * <ul>
     *   <li>{@code driverShare} 0: the rider is debited {@code X} and the platform credited
     *       {@code X}; no driver posting.</li>
     *   <li>{@code driverShare} 1: the rider is debited {@code X} and the driver credited
     *       {@code X}; no platform posting.</li>
     *   <li>{@code X} 0.01 and {@code driverShare} 0.80: {@code D} rounds to 0.01, so the rider is
     *       debited 0.01 and the driver credited 0.01; the platform's remainder is zero and its
     *       posting is omitted.</li>
     * </ul>
     *
     * @param driverShare fraction of the fare that goes to the driver, between 0 and 1 inclusive
     */
    public static JournalEntry settlement(String rideId, String sourceEventId, Money quote, BigDecimal driverShare) {
        if (driverShare == null || driverShare.signum() < 0 || driverShare.compareTo(BigDecimal.ONE) > 0) {
            throw new IllegalArgumentException("driver share must be between 0 and 1, got " + driverShare);
        }
        Money driver = quote.times(driverShare);
        Money platform = quote.plus(driver.negate());
        List<Posting> postings = new ArrayList<>(3);
        postings.add(Posting.debit(Account.RIDER_RECEIVABLE, quote));
        if (!driver.isZero()) {
            postings.add(Posting.credit(Account.DRIVER_PAYABLE, driver));
        }
        if (!platform.isZero()) {
            postings.add(Posting.credit(Account.PLATFORM_REVENUE, platform));
        }
        return new JournalEntry(rideId, JournalKind.SETTLEMENT, sourceEventId, postings);
    }
}
