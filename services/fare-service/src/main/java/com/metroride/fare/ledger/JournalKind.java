package com.metroride.fare.ledger;

/**
 * Why a journal entry exists. Stored as the text code in {@code fare.journal_entries.kind}.
 *
 * <p>{@link #QUOTE_HOLD} is written on {@code ride_assigned}; {@link #HOLD_REVERSAL} and
 * {@link #SETTLEMENT} are written together, in one transaction, on {@code ride_completed}.
 */
public enum JournalKind {
    /** On {@code ride_assigned}: the quoted fare is held against the rider. */
    QUOTE_HOLD("quote_hold"),
    /** On {@code ride_completed}: the quote hold is reversed posting by posting. */
    HOLD_REVERSAL("hold_reversal"),
    /** On {@code ride_completed}: the quoted fare is moved into driver payable and platform revenue. */
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
