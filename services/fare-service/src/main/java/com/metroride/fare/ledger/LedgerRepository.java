package com.metroride.fare.ledger;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * Append-only access to {@code fare.journal_entries} and {@code fare.postings}. There is no update
 * and no delete on purpose: a ledger is corrected by appending a reversing entry, never by
 * editing history.
 *
 * <p>Plain JDBC through {@link JdbcClient} rather than JPA entities: the domain types are immutable
 * records, and an insert-only table has no use for a managed entity lifecycle. The statements run
 * on the connection of the surrounding Spring transaction (the one {@code ProcessedEventRecorder}
 * opens), so they share its commit, rollback and timeout with the {@code processed_events} insert.
 */
@Repository
public class LedgerRepository {

    // Left join on purpose: an entry without postings must surface and be rejected by the
    // JournalEntry constructor, not vanish from the result as an inner join would make it.
    private static final String SELECT_BY_RIDE = """
            select j.id, j.ride_id, j.kind, j.source_event_id, j.created_at, p.id as posting_id, p.account, p.amount
            from fare.journal_entries j
            left join fare.postings p on p.journal_entry_id = j.id
            where j.ride_id = :rideId
            order by j.id, p.id
            """;

    // The same rows restricted to one kind, with the journal rows locked for the rest of the
    // transaction. "for update of j": the postings side of a left join cannot be locked, and the
    // journal row is the lock that matters. Two transactions settling the same ride serialise on
    // it, and the second sees the first's settlement once it has the lock.
    private static final String LOCK_BY_RIDE_AND_KIND = """
            select j.id, j.ride_id, j.kind, j.source_event_id, j.created_at, p.id as posting_id, p.account, p.amount
            from fare.journal_entries j
            left join fare.postings p on p.journal_entry_id = j.id
            where j.ride_id = :rideId and j.kind = :kind
            order by j.id, p.id
            for update of j
            """;

    private final JdbcClient jdbc;

    public LedgerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the entry and its postings. Must be called inside a transaction.
     *
     * @return the generated {@code journal_entries.id}
     * @throws LedgerConflictException when the ride already has an entry of this kind and the
     *                                 per-ride unique index refused this one; any other integrity
     *                                 violation is rethrown as Spring reports it
     */
    public long append(JournalEntry entry, Instant createdAt) {
        KeyHolder key = new GeneratedKeyHolder();
        try {
            jdbc.sql("""
                    insert into fare.journal_entries (ride_id, kind, source_event_id, created_at)
                    values (:rideId, :kind, :sourceEventId, :createdAt)
                    """)
                    .param("rideId", entry.rideId())
                    .param("kind", entry.kind().code())
                    .param("sourceEventId", entry.sourceEventId())
                    .param("createdAt", java.sql.Timestamp.from(createdAt))
                    .update(key, "id");
        } catch (DuplicateKeyException duplicate) {
            throw perRideConflict(entry, duplicate).orElse(duplicate);
        }
        long journalId = key.getKeyAs(Long.class);
        for (Posting posting : entry.postings()) {
            jdbc.sql("""
                    insert into fare.postings (journal_entry_id, account, amount)
                    values (:journalId, :account, :amount)
                    """)
                    .param("journalId", journalId)
                    .param("account", posting.account().code())
                    .param("amount", posting.amount().amount())
                    .update();
        }
        return journalId;
    }

    /**
     * All entries on one ride in insertion order, each rebuilt through the {@link JournalEntry}
     * constructor.
     *
     * @throws CorruptLedgerException when the rows do not form valid entries (see {@link #collect})
     */
    public List<StoredJournalEntry> findByRideId(String rideId) {
        return jdbc.sql(SELECT_BY_RIDE)
                .param("rideId", rideId)
                .query(LedgerRepository::collect);
    }

    /**
     * The ride's {@code quote_hold} entries, with their journal rows locked ({@code select ... for
     * update}) until the surrounding transaction ends. Must be called inside a transaction; the
     * lock wait is bounded by the transaction's timeout like every other statement. Settlement
     * calls this first, so two instances handling two different completion events for one ride
     * serialise here, and the second finds the first's settlement with {@link #hasSettlement}.
     *
     * @throws CorruptLedgerException when the rows do not form valid entries (see {@link #collect})
     */
    public List<StoredJournalEntry> lockQuoteHolds(String rideId) {
        return jdbc.sql(LOCK_BY_RIDE_AND_KIND)
                .param("rideId", rideId)
                .param("kind", JournalKind.QUOTE_HOLD.code())
                .query(LedgerRepository::collect);
    }

    /**
     * The per-ride unique indexes of V3 are the only constraints a well-formed entry can violate,
     * and they name the ride's ledger state, not a deployment fault, so they get their own
     * exception. The violated constraint's name is in the driver's error; anything else (the
     * {@code (source_event_id, kind)} key, a foreign key) is left as the {@link DuplicateKeyException}
     * Spring raised, which the consumer treats as fatal.
     */
    private static Optional<RuntimeException> perRideConflict(JournalEntry entry, DuplicateKeyException duplicate) {
        Throwable root = NestedExceptionUtils.getMostSpecificCause(duplicate);
        if (!(root instanceof PSQLException psql)) {
            return Optional.empty();
        }
        ServerErrorMessage error = psql.getServerErrorMessage();
        if (error == null || error.getConstraint() == null) {
            return Optional.empty();
        }
        return LedgerConflictException.Conflict.forConstraint(error.getConstraint())
                .map(conflict -> new LedgerConflictException(conflict,
                        "ride " + entry.rideId() + " already has a " + entry.kind().code()
                                + "; entry from event " + entry.sourceEventId() + " refused by " + conflict.indexName(),
                        duplicate));
    }

    /** Whether the ride already has a {@code settlement} entry, from any source event. */
    public boolean hasSettlement(String rideId) {
        Integer count = jdbc.sql("select count(*) from fare.journal_entries where ride_id = :rideId and kind = :kind")
                .param("rideId", rideId)
                .param("kind", JournalKind.SETTLEMENT.code())
                .query(Integer.class)
                .single();
        return count != null && count > 0;
    }

    /**
     * Rebuilds entries from the joined rows. The {@link JournalEntry} constructor (and the code
     * lookups) refuse rows that do not form a valid entry with an {@code IllegalArgumentException};
     * inside a {@code @Repository} Spring would translate that into
     * {@code InvalidDataAccessApiUsageException}, which the consumer treats as a fatal
     * misconfiguration and halts on. A ledger whose rows are wrong is a data problem on one ride,
     * not a deployment problem, so it is reported as a {@link CorruptLedgerException} instead,
     * which Spring leaves untranslated and the consumer quarantines.
     */
    private static List<StoredJournalEntry> collect(ResultSet rs) throws SQLException {
        try {
            return rebuild(rs);
        } catch (IllegalArgumentException invalid) {
            throw new CorruptLedgerException("ledger rows do not form a valid entry: " + invalid.getMessage(), invalid);
        }
    }

    private static List<StoredJournalEntry> rebuild(ResultSet rs) throws SQLException {
        Map<Long, EntryRows> byId = new LinkedHashMap<>();
        while (rs.next()) {
            long id = rs.getLong("id");
            EntryRows rows = byId.get(id);
            if (rows == null) {
                rows = new EntryRows(
                        rs.getString("ride_id"),
                        rs.getString("kind"),
                        rs.getString("source_event_id"),
                        rs.getObject("created_at", java.time.OffsetDateTime.class).toInstant());
                byId.put(id, rows);
            }
            String account = rs.getString("account");
            if (account != null) {
                BigDecimal amount = rs.getBigDecimal("amount");
                rows.postings.add(new Posting(Account.fromCode(account), Money.of(amount)));
            }
        }
        List<StoredJournalEntry> result = new ArrayList<>(byId.size());
        byId.forEach((id, rows) -> result.add(new StoredJournalEntry(id, rows.createdAt, new JournalEntry(
                rows.rideId, JournalKind.fromCode(rows.kind), rows.sourceEventId, rows.postings))));
        return result;
    }

    private static final class EntryRows {
        final String rideId;
        final String kind;
        final String sourceEventId;
        final Instant createdAt;
        final List<Posting> postings = new ArrayList<>();

        EntryRows(String rideId, String kind, String sourceEventId, Instant createdAt) {
            this.rideId = rideId;
            this.kind = kind;
            this.sourceEventId = sourceEventId;
            this.createdAt = createdAt;
        }
    }
}
