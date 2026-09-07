package com.metroride.fare.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JournalEntryTest {

    @Test
    void quoteHoldDebitsTheRiderAndCreditsTheHold() {
        JournalEntry entry = JournalEntry.quoteHold("ride-1", "event-1", Money.of("5.85"));

        assertThat(entry.kind()).isEqualTo(JournalKind.QUOTE_HOLD);
        assertThat(entry.postings()).containsExactly(
                new Posting(Account.RIDER_RECEIVABLE, Money.of("5.85")),
                new Posting(Account.FARE_HOLD, Money.of("-5.85")));
    }

    @Test
    void unbalancedPostingsCannotBeConstructed() {
        List<Posting> unbalanced = List.of(
                Posting.debit(Account.RIDER_RECEIVABLE, Money.of("5.85")),
                Posting.credit(Account.FARE_HOLD, Money.of("5.84")));

        assertThatThrownBy(() -> new JournalEntry("ride-1", JournalKind.QUOTE_HOLD, "event-1", unbalanced))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sum to 0.01, not zero");
    }

    @Test
    void oneSidedEntryCannotBeConstructed() {
        List<Posting> oneSided = List.of(Posting.debit(Account.RIDER_RECEIVABLE, Money.of("5.85")));

        assertThatThrownBy(() -> new JournalEntry("ride-1", JournalKind.QUOTE_HOLD, "event-1", oneSided))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not zero");
    }

    @Test
    void emptyPostingsCannotBeConstructed() {
        assertThatThrownBy(() -> new JournalEntry("ride-1", JournalKind.QUOTE_HOLD, "event-1", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one posting");
        assertThatThrownBy(() -> new JournalEntry("ride-1", JournalKind.QUOTE_HOLD, "event-1", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresRideIdKindAndSourceEvent() {
        List<Posting> balanced = JournalEntry.quoteHold("ride-1", "event-1", Money.of("1.00")).postings();

        assertThatThrownBy(() -> new JournalEntry(" ", JournalKind.QUOTE_HOLD, "event-1", balanced))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JournalEntry("ride-1", null, "event-1", balanced))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JournalEntry("ride-1", JournalKind.QUOTE_HOLD, "", balanced))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void postingsAreCopiedAndImmutable() {
        List<Posting> source = new ArrayList<>(JournalEntry.quoteHold("ride-1", "event-1", Money.of("1.00")).postings());
        JournalEntry entry = new JournalEntry("ride-1", JournalKind.QUOTE_HOLD, "event-1", source);

        source.clear();

        assertThat(entry.postings()).hasSize(2);
        assertThatThrownBy(() -> entry.postings().add(Posting.debit(Account.FARE_HOLD, Money.of("1.00"))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void postingsRejectZeroAmountsAndSignsThatContradictTheirSide() {
        assertThatThrownBy(() -> new Posting(Account.FARE_HOLD, Money.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zero amount");
        assertThatThrownBy(() -> Posting.debit(Account.FARE_HOLD, Money.of("-1.00")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Posting.credit(Account.FARE_HOLD, Money.of("-1.00")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
