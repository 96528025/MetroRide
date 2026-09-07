package com.metroride.fare.ledger;

/**
 * One line of a journal entry: an amount moved on one account. Positive is a debit, negative a
 * credit. A posting never stands alone; it is always part of a balanced {@link JournalEntry}.
 *
 * @param account the account touched
 * @param amount  the signed amount; zero is rejected because a posting that moves nothing is a bug
 */
public record Posting(Account account, Money amount) {

    public Posting {
        if (account == null) {
            throw new IllegalArgumentException("posting account is required");
        }
        if (amount == null) {
            throw new IllegalArgumentException("posting amount is required");
        }
        if (amount.isZero()) {
            throw new IllegalArgumentException("posting on " + account.code() + " has a zero amount");
        }
    }

    public static Posting debit(Account account, Money amount) {
        if (amount.isNegative()) {
            throw new IllegalArgumentException("debit amount must be positive, got " + amount);
        }
        return new Posting(account, amount);
    }

    public static Posting credit(Account account, Money amount) {
        if (amount.isNegative()) {
            throw new IllegalArgumentException("credit amount must be positive, got " + amount);
        }
        return new Posting(account, amount.negate());
    }
}
