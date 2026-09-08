package com.metroride.fare.ledger;

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
}
