package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.pricing.FareQuoteException;
import com.metroride.fare.pricing.FareQuoteException.Reason;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TypeMismatchDataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;

/** One case per branch of {@link FailureClass#of}, including the subclasses the consumer really sees. */
class FailureClassTest {

    @Test
    void anUndecodableEntryIsPoison() {
        assertThat(FailureClass.of(new EnvelopeDecodeException("event envelope in message 1-0 has no id")))
                .isEqualTo(FailureClass.POISON);
    }

    @Test
    void aPayloadThatCannotBeQuotedIsPoisonWhateverTheReason() {
        assertThat(FailureClass.of(new FareQuoteException(Reason.PAYLOAD, "no ride_id", null)))
                .isEqualTo(FailureClass.POISON);
        assertThat(FailureClass.of(new FareQuoteException(Reason.CALCULATION, "distance must not be negative", null)))
                .isEqualTo(FailureClass.POISON);
    }

    @Test
    void aTransientDataAccessFailureIsRetryable() {
        // The lock-wait cancellation the transaction timeout produces, and a deadlock.
        assertThat(FailureClass.of(new QueryTimeoutException("canceling statement due to user request")))
                .isEqualTo(FailureClass.RETRYABLE);
        assertThat(FailureClass.of(new DeadlockLoserDataAccessException("deadlock detected", new SQLException())))
                .isEqualTo(FailureClass.RETRYABLE);
    }

    @Test
    void aLostConnectionIsRetryableAlthoughSpringFilesItAsNonTransient() {
        assertThat(FailureClass.of(new DataAccessResourceFailureException("connection refused")))
                .isEqualTo(FailureClass.RETRYABLE);
    }

    @Test
    void aTransactionFailureIsRetryable() {
        assertThat(FailureClass.of(new CannotCreateTransactionException("pool exhausted")))
                .isEqualTo(FailureClass.RETRYABLE);
        assertThat(FailureClass.of(new TransactionSystemException("commit failed")))
                .isEqualTo(FailureClass.RETRYABLE);
    }

    @Test
    void wrongSqlOrSchemaIsFatal() {
        assertThat(FailureClass.of(new BadSqlGrammarException("insert", "insert into fare.nope", new SQLException())))
                .isEqualTo(FailureClass.FATAL);
        assertThat(FailureClass.of(new TypeMismatchDataAccessException("column amount is text")))
                .isEqualTo(FailureClass.FATAL);
    }

    @Test
    void misuseOfTheDataAccessApiIsFatal() {
        // What @Repository translation makes of an IllegalArgumentException thrown inside a repository.
        assertThat(FailureClass.of(new InvalidDataAccessApiUsageException("postings must balance")))
                .isEqualTo(FailureClass.FATAL);
    }

    @Test
    void aViolatedConstraintIsFatal() {
        // Not retryable: the writer cannot hit a constraint unless the schema or the data is
        // already wrong, and the only unique key it could reach implies a committed event row
        // that the idempotency insert would have reported as a duplicate first.
        assertThat(FailureClass.of(new DataIntegrityViolationException("null value in column ride_id",
                new SQLException("null value in column \"ride_id\" violates not-null constraint", "23502"))))
                .isEqualTo(FailureClass.FATAL);
        assertThat(FailureClass.of(new DuplicateKeyException("journal_entries_source_event_id_kind_key",
                new SQLException("duplicate key value violates unique constraint", "23505"))))
                .isEqualTo(FailureClass.FATAL);
        // No SQLSTATE to tell the two apart: the safe reading is the deployment's fault.
        assertThat(FailureClass.of(new DataIntegrityViolationException("unknown")))
                .isEqualTo(FailureClass.FATAL);
    }

    /**
     * Spring files SQL "data exceptions" under the same class as constraint violations, but they
     * are the entry's fault: PostgreSQL refuses the same bytes on every delivery, and one crafted
     * entry must not be able to halt the service.
     */
    @Test
    void aRefusedValueIsPoisonNotFatal() {
        // pgjdbc refuses a NUL character in a string parameter before the statement is sent.
        assertThat(FailureClass.of(new DataIntegrityViolationException("could not execute statement",
                new SQLException("Zero bytes may not occur in string parameters.", "22023"))))
                .isEqualTo(FailureClass.POISON);
        // The server's own class-22 states: invalid text encoding, numeric overflow.
        assertThat(FailureClass.of(new DataIntegrityViolationException("could not execute statement",
                new SQLException("invalid byte sequence for encoding \"UTF8\"", "22021"))))
                .isEqualTo(FailureClass.POISON);
        assertThat(FailureClass.of(new DataIntegrityViolationException("could not execute statement",
                new RuntimeException("wrapped", new SQLException("numeric field overflow", "22003")))))
                .as("the SQLState is read from the root of the chain").isEqualTo(FailureClass.POISON);
    }

    @Test
    void anUnforeseenExceptionIsRetryableUpToTheDeliveryCap() {
        assertThat(FailureClass.of(new NullPointerException())).isEqualTo(FailureClass.RETRYABLE);
        assertThat(FailureClass.of(new IllegalStateException("bug"))).isEqualTo(FailureClass.RETRYABLE);
    }
}
