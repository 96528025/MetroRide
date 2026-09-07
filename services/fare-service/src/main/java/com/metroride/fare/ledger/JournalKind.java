package com.metroride.fare.ledger;

/**
 * Why a journal entry exists. Stored as the text code in {@code fare.journal_entries.kind}.
 *
 * <p>Only {@link #QUOTE_HOLD} is written today. {@link #HOLD_REVERSAL} and {@link #SETTLEMENT} are
 * declared so the vocabulary is fixed, but nothing produces them until {@code ride_completed} is
 * consumed.
 */
public enum JournalKind {
    /** On {@code ride_assigned}: the quoted fare is held against the rider. */
    QUOTE_HOLD("quote_hold"),
    /** Reverses an earlier hold with the opposite postings; not produced yet. */
    HOLD_REVERSAL("hold_reversal"),
    /** Moves a settled fare into driver payable and platform revenue; not produced yet. */
    SETTLEMENT("settlement");

    private final String code;

    JournalKind(String code) {
        this.code = code;
    }

    /** The value stored in {@code fare.journal_entries.kind}. */
    public String code() {
        return code;
    }

    public static JournalKind fromCode(String code) {
        for (JournalKind kind : values()) {
            if (kind.code.equals(code)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown journal kind " + code);
    }
}
