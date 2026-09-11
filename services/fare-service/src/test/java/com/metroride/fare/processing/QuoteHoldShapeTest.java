package com.metroride.fare.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.metroride.fare.ledger.Account;
import com.metroride.fare.ledger.CorruptLedgerException;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.Money;
import com.metroride.fare.ledger.Posting;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The shape check works on the posting list rather than on a {@link JournalEntry}, because the
 * cases it must refuse (a missing posting, a duplicated one, a wrong account, a wrong side) are
 * not all constructible as a balanced entry; the balance rule stays in the constructor.
 */
class QuoteHoldShapeTest {

    private static final Money X = Money.of("5.85");

    @Test
    void theHoldTheServiceWritesYieldsItsAmount() {
        List<Posting> postings = JournalEntry.quoteHold("ride-1", "event-1", X).postings();

        assertThat(QuoteHoldShape.amountOf("ride-1", postings)).isEqualTo(X);
        // Order does not matter, accounts and sides do.
        assertThat(QuoteHoldShape.amountOf("ride-1", List.of(postings.get(1), postings.get(0)))).isEqualTo(X);
    }

    @Test
    void aMissingPostingIsCorrupt() {
        assertThatThrownBy(() -> QuoteHoldShape.amountOf("ride-1", List.of(Posting.debit(Account.RIDER_RECEIVABLE, X))))
                .isInstanceOf(CorruptLedgerException.class)
                .hasMessageContaining("ride-1")
                .hasMessageContaining("1 postings");
    }

    @Test
    void aDuplicatedPostingIsCorrupt() {
        // Three lines: one too many.
        assertThatThrownBy(() -> QuoteHoldShape.amountOf("ride-1", List.of(
                Posting.debit(Account.RIDER_RECEIVABLE, X),
                Posting.debit(Account.RIDER_RECEIVABLE, X),
                Posting.credit(Account.FARE_HOLD, Money.of("11.70")))))
                .isInstanceOf(CorruptLedgerException.class)
                .hasMessageContaining("3 postings");
        // Two lines, but both on the rider: the hold account is never posted.
        assertThatThrownBy(() -> QuoteHoldShape.amountOf("ride-1", List.of(
                Posting.debit(Account.RIDER_RECEIVABLE, X),
                Posting.credit(Account.RIDER_RECEIVABLE, X))))
                .isInstanceOf(CorruptLedgerException.class)
                .hasMessageContaining("once each");
    }

    @Test
    void aWrongAccountIsCorrupt() {
        assertThatThrownBy(() -> QuoteHoldShape.amountOf("ride-1", List.of(
                Posting.debit(Account.RIDER_RECEIVABLE, X),
                Posting.credit(Account.DRIVER_PAYABLE, X))))
                .isInstanceOf(CorruptLedgerException.class)
                .hasMessageContaining("driver_payable");
    }

    @Test
    void aWrongSideIsCorrupt() {
        assertThatThrownBy(() -> QuoteHoldShape.amountOf("ride-1", List.of(
                Posting.credit(Account.RIDER_RECEIVABLE, X),
                Posting.debit(Account.FARE_HOLD, X))))
                .isInstanceOf(CorruptLedgerException.class)
                .hasMessageContaining("must debit rider_receivable");
    }

    @Test
    void unequalAmountsAreCorrupt() {
        assertThatThrownBy(() -> QuoteHoldShape.amountOf("ride-1", List.of(
                Posting.debit(Account.RIDER_RECEIVABLE, X),
                Posting.credit(Account.FARE_HOLD, Money.of("5.84")))))
                .isInstanceOf(CorruptLedgerException.class)
                .hasMessageContaining("same amount");
    }
}
