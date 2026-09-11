package com.metroride.fare.ledger;

import com.metroride.fare.consumer.ClassifiedFailure;
import com.metroride.fare.consumer.DeadLetterReason;
import com.metroride.fare.consumer.FailureClass;
import java.util.Optional;

/**
 * Rows already in the ledger do not describe a valid entry: a journal entry without postings, a
 * posting set that does not balance, an unknown account or kind, or a {@code quote_hold} whose
 * postings are not the two the {@link JournalEntry#quoteHold} factory writes. The fault is the
 * ride's ledger, not the message that made the service read it, so the consumer quarantines the
 * message (dead-letters it under {@code corrupt_hold} and acknowledges it) instead of retrying
 * against an append-only ledger that no retry can repair, and instead of halting on one ride.
 *
 * <p>Deliberately not a {@code DataAccessException} and not an {@code IllegalArgumentException}:
 * Spring's {@code @Repository} translation turns the latter into
 * {@code InvalidDataAccessApiUsageException}, which the consumer treats as fatal and halts on.
 * This class passes through the translation untouched.
 */
public class CorruptLedgerException extends RuntimeException implements ClassifiedFailure {

    public CorruptLedgerException(String message) {
        super(message);
    }

    public CorruptLedgerException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public FailureClass failureClass() {
        return FailureClass.QUARANTINE;
    }

    @Override
    public Optional<DeadLetterReason> deadLetterReason() {
        return Optional.of(DeadLetterReason.CORRUPT_HOLD);
    }
}
