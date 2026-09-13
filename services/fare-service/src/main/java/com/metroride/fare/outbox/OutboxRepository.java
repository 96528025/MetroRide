package com.metroride.fare.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.FareServiceApplication;
import com.metroride.fare.events.Envelope;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * The statements of {@code shared/pkg/outbox/outbox.go}, against {@code fare.event_outbox}.
 *
 * <p>{@link #enqueue} runs in the caller's transaction (the settlement), so a row exists exactly
 * when the journal entries it announces exist. The other methods are the relay's; they run in the
 * relay's own transaction, one batch per pass. Every statement carries its own query timeout of
 * {@code metroride.postgres.timeout-seconds}, the way each {@code tx.Exec} in the Go relay carries
 * its own {@code WithPostgresTimeout} context; the relay transaction itself has no overall budget,
 * because a batch that publishes slowly must still be able to record its failures (see
 * {@link OutboxRelay}). Inside the settlement transaction the shorter remaining transaction time
 * wins, as for every other statement there.
 */
@Repository
public class OutboxRepository {

    /** One row as the relay sees it. */
    public record PendingEvent(String id, String stream, String envelopeJson, int publishAttempts) {
    }

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OutboxRepository(DataSource dataSource, ObjectMapper mapper, int statementTimeoutSeconds) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.jdbc.setQueryTimeout(statementTimeoutSeconds);
        this.mapper = mapper;
    }

    /** Inserts the envelope for {@code stream}; must be called inside the transaction that produced it. */
    public void enqueue(String stream, Envelope envelope, Instant createdAt) {
        String body;
        try {
            body = mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("encode outbox envelope " + envelope.id(), e);
        }
        jdbc.update("""
                insert into fare.event_outbox (id, source_service, aggregate_id, event_type, stream, envelope, created_at)
                values (?, ?, ?, ?, ?, cast(? as jsonb), ?)
                """,
                envelope.id(), envelope.source(), envelope.correlationId(), envelope.type(), stream, body,
                Timestamp.from(createdAt));
    }

    /**
     * The batch the Go relay takes: unpublished rows whose retry time has come, oldest retry time
     * first, locked for this transaction and skipped by any other relay pass.
     */
    public List<PendingEvent> lockPending(int batchSize) {
        return jdbc.query("""
                select id, stream, envelope::text, publish_attempts
                from fare.event_outbox
                where source_service = ? and published_at is null and next_attempt_at <= now()
                order by next_attempt_at, created_at, id
                limit ?
                for update skip locked
                """,
                (rs, row) -> new PendingEvent(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4)),
                FareServiceApplication.SERVICE_NAME, batchSize);
    }

    /** Records a confirmed publication. A failure here aborts the relay transaction; see {@link OutboxRelay}. */
    public void markPublished(PendingEvent event) throws DataAccessException {
        jdbc.update("""
                update fare.event_outbox
                set published_at = clock_timestamp(), publish_attempts = publish_attempts + 1, last_error = null
                where id = ? and stream = ?
                """, event.id(), event.stream());
    }

    /** Records a failed attempt and schedules the next one {@code retryDelaySeconds} from now. */
    public void markFailed(PendingEvent event, double retryDelaySeconds, String error) throws DataAccessException {
        jdbc.update("""
                update fare.event_outbox
                set publish_attempts = publish_attempts + 1,
                    next_attempt_at = clock_timestamp() + make_interval(secs => ?),
                    last_error = ?
                where id = ? and stream = ? and published_at is null
                """, retryDelaySeconds, error, event.id(), event.stream());
    }

    /** Rows not yet published, for the {@code metroride_fare_outbox_unpublished} gauge. */
    public long countUnpublished() {
        Long count = jdbc.queryForObject(
                "select count(*) from fare.event_outbox where source_service = ? and published_at is null",
                Long.class, FareServiceApplication.SERVICE_NAME);
        return count == null ? 0 : count;
    }
}
