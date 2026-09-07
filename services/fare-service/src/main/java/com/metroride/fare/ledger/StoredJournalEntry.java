package com.metroride.fare.ledger;

import java.time.Instant;

/**
 * A {@link JournalEntry} read back from the database together with the columns the database
 * assigned. Reads rebuild the entry through its constructor, so a row set that no longer balances
 * fails loudly instead of being served.
 *
 * @param id        {@code fare.journal_entries.id}
 * @param createdAt {@code fare.journal_entries.created_at}
 * @param entry     the balanced entry
 */
public record StoredJournalEntry(long id, Instant createdAt, JournalEntry entry) {
}
