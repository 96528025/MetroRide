package com.metroride.fare.processing;

import com.metroride.fare.ledger.Account;
import com.metroride.fare.ledger.CorruptLedgerException;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.Money;
import com.metroride.fare.ledger.Posting;
import java.util.List;

/**
 * The shape a {@code quote_hold} must have before it is used for settlement: exactly the two
 * postings {@link JournalEntry#quoteHold} writes, a positive debit on {@code rider_receivable} and
 * a credit of the same amount on {@code fare_hold}. Balance is not checked here; the
 * {@link JournalEntry} constructor already refused any row set that does not balance when the
 * hold was read back. This class checks what balance cannot: that the amount is on the accounts
 * and the sides the settlement is about to reverse. Anything else is a
 * {@link CorruptLedgerException}, which the consumer quarantines under {@code corrupt_hold}.
 */
final class QuoteHoldShape {

    private QuoteHoldShape() {
    }

    /** @return the quoted amount {@code X}, the debit on {@code rider_receivable} */
    static Money amountOf(String rideId, List<Posting> postings) {
        if (postings.size() != 2) {
            throw new CorruptLedgerException("quote_hold of ride " + rideId + " has " + postings.size()
                    + " postings, not the 2 the service writes");
        }
        Posting rider = null;
        Posting hold = null;
        for (Posting posting : postings) {
            if (posting.account() == Account.RIDER_RECEIVABLE) {
                rider = posting;
            } else if (posting.account() == Account.FARE_HOLD) {
                hold = posting;
            } else {
                throw new CorruptLedgerException("quote_hold of ride " + rideId + " posts to "
                        + posting.account().code() + "; only rider_receivable and fare_hold are allowed");
            }
        }
        if (rider == null || hold == null) {
            throw new CorruptLedgerException("quote_hold of ride " + rideId
                    + " must post to rider_receivable and fare_hold once each");
        }
        if (rider.amount().isNegative() || !rider.amount().plus(hold.amount()).isZero()) {
            throw new CorruptLedgerException("quote_hold of ride " + rideId + " must debit rider_receivable and credit"
                    + " fare_hold the same amount, got " + rider.amount() + " and " + hold.amount());
        }
        return rider.amount();
    }
}
