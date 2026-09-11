package com.metroride.fare.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
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
    void holdReversalIsTheQuoteHoldPostingForPostingWithTheOppositeSign() {
        JournalEntry hold = JournalEntry.quoteHold("ride-1", "event-1", Money.of("5.85"));
        JournalEntry reversal = JournalEntry.holdReversal("ride-1", "event-2", Money.of("5.85"));

        assertThat(reversal.kind()).isEqualTo(JournalKind.HOLD_REVERSAL);
        assertThat(reversal.sourceEventId()).isEqualTo("event-2");
        assertThat(reversal.postings()).hasSameSizeAs(hold.postings());
        for (int i = 0; i < hold.postings().size(); i++) {
            assertThat(reversal.postings().get(i).account()).isEqualTo(hold.postings().get(i).account());
            assertThat(reversal.postings().get(i).amount()).isEqualTo(hold.postings().get(i).amount().negate());
        }
        assertThat(reversal.postings()).containsExactly(
                new Posting(Account.RIDER_RECEIVABLE, Money.of("-5.85")),
                new Posting(Account.FARE_HOLD, Money.of("5.85")));
    }

    /** X = 5.85, share 0.80: D = 4.68 (4.68 exactly), platform 1.17; the three sum to zero. */
    @Test
    void settlementDebitsTheRiderAndSplitsTheQuoteBetweenDriverAndPlatform() {
        JournalEntry settlement = JournalEntry.settlement("ride-1", "event-2", Money.of("5.85"), new BigDecimal("0.80"));

        assertThat(settlement.kind()).isEqualTo(JournalKind.SETTLEMENT);
        assertThat(settlement.postings()).containsExactly(
                new Posting(Account.RIDER_RECEIVABLE, Money.of("5.85")),
                new Posting(Account.DRIVER_PAYABLE, Money.of("-4.68")),
                new Posting(Account.PLATFORM_REVENUE, Money.of("-1.17")));
    }

    /** The platform takes the remainder after one rounding, so an odd cent never disappears or doubles. */
    @Test
    void settlementGivesTheRoundingRemainderToThePlatform() {
        // 0.80 * 5.55 = 4.44 exactly; 0.80 * 5.57 = 4.456 -> 4.46, platform 1.11 not 1.114.
        JournalEntry settlement = JournalEntry.settlement("ride-1", "event-2", Money.of("5.57"), new BigDecimal("0.80"));

        assertThat(settlement.postings()).containsExactly(
                new Posting(Account.RIDER_RECEIVABLE, Money.of("5.57")),
                new Posting(Account.DRIVER_PAYABLE, Money.of("-4.46")),
                new Posting(Account.PLATFORM_REVENUE, Money.of("-1.11")));
    }

    /** X = 0.01, share 0.80: D rounds to 0.01 and the platform's zero posting is omitted. */
    @Test
    void settlementOmitsAZeroPlatformRemainder() {
        JournalEntry settlement = JournalEntry.settlement("ride-1", "event-2", Money.of("0.01"), new BigDecimal("0.80"));

        assertThat(settlement.postings()).containsExactly(
                new Posting(Account.RIDER_RECEIVABLE, Money.of("0.01")),
                new Posting(Account.DRIVER_PAYABLE, Money.of("-0.01")));
    }

    @Test
    void settlementWithShareZeroCreditsOnlyThePlatform() {
        JournalEntry settlement = JournalEntry.settlement("ride-1", "event-2", Money.of("5.85"), BigDecimal.ZERO);

        assertThat(settlement.postings()).containsExactly(
                new Posting(Account.RIDER_RECEIVABLE, Money.of("5.85")),
                new Posting(Account.PLATFORM_REVENUE, Money.of("-5.85")));
    }

    @Test
    void settlementWithShareOneCreditsOnlyTheDriver() {
        JournalEntry settlement = JournalEntry.settlement("ride-1", "event-2", Money.of("5.85"), BigDecimal.ONE);

        assertThat(settlement.postings()).containsExactly(
                new Posting(Account.RIDER_RECEIVABLE, Money.of("5.85")),
                new Posting(Account.DRIVER_PAYABLE, Money.of("-5.85")));
    }

    @Test
    void settlementRejectsAShareOutsideZeroToOne() {
        assertThatThrownBy(() -> JournalEntry.settlement("ride-1", "event-2", Money.of("5.85"), new BigDecimal("1.01")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JournalEntry.settlement("ride-1", "event-2", Money.of("5.85"), new BigDecimal("-0.1")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JournalEntry.settlement("ride-1", "event-2", Money.of("5.85"), null))
                .isInstanceOf(IllegalArgumentException.class);
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
