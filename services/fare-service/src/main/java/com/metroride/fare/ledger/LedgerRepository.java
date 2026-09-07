package com.metroride.fare.ledger;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private static final String SELECT_BY_RIDE = """
            select j.id, j.ride_id, j.kind, j.source_event_id, j.created_at, p.id as posting_id, p.account, p.amount
            from fare.journal_entries j
            join fare.postings p on p.journal_entry_id = j.id
            where j.ride_id = :rideId
            order by j.id, p.id
            """;

    private final JdbcClient jdbc;

    public LedgerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the entry and its postings. Must be called inside a transaction.
     *
     * @return the generated {@code journal_entries.id}
     */
    public long append(JournalEntry entry, Instant createdAt) {
        KeyHolder key = new GeneratedKeyHolder();
        jdbc.sql("""
                insert into fare.journal_entries (ride_id, kind, source_event_id, created_at)
                values (:rideId, :kind, :sourceEventId, :createdAt)
                """)
                .param("rideId", entry.rideId())
                .param("kind", entry.kind().code())
                .param("sourceEventId", entry.sourceEventId())
                .param("createdAt", java.sql.Timestamp.from(createdAt))
                .update(key, "id");
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

    /** All entries on one ride in insertion order, each rebuilt through the {@link JournalEntry} constructor. */
    public List<StoredJournalEntry> findByRideId(String rideId) {
        return jdbc.sql(SELECT_BY_RIDE)
                .param("rideId", rideId)
                .query(LedgerRepository::collect);
    }

    private static List<StoredJournalEntry> collect(ResultSet rs) throws SQLException {
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
            BigDecimal amount = rs.getBigDecimal("amount");
            rows.postings.add(new Posting(Account.fromCode(rs.getString("account")), Money.of(amount)));
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
