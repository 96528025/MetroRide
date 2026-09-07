package com.metroride.fare.ledger;

/**
 * The ledger accounts a posting can touch. Stored as the text code in {@code fare.postings.account};
 * there is no accounts table because nothing needs per-account attributes yet.
 *
 * <p>Sign convention: debits are positive, credits negative. A quote hold debits
 * {@link #RIDER_RECEIVABLE} (the rider owes the quoted fare) and credits {@link #FARE_HOLD} (the
 * fare is reserved, not yet earned). Settlement later moves the hold into {@link #DRIVER_PAYABLE}
 * and {@link #PLATFORM_REVENUE}.
 */
public enum Account {
    RIDER_RECEIVABLE("rider_receivable"),
    FARE_HOLD("fare_hold"),
    DRIVER_PAYABLE("driver_payable"),
    PLATFORM_REVENUE("platform_revenue");

    private final String code;

    Account(String code) {
        this.code = code;
    }

    /** The value stored in {@code fare.postings.account}. */
    public String code() {
        return code;
    }

    public static Account fromCode(String code) {
        for (Account account : values()) {
            if (account.code.equals(code)) {
                return account;
            }
        }
        throw new IllegalArgumentException("unknown ledger account " + code);
    }
}
