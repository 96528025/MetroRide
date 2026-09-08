package com.metroride.fare.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.ledger.Posting;
import com.metroride.fare.ledger.StoredJournalEntry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only view of one ride's ledger, in the style of the Go services' handlers
 * ({@code GET /v1/rides/{ride_id}} in rider-service): snake_case JSON, {@code {"error": ...}} with
 * a 404 when there is nothing to show.
 *
 * <pre>
 *   GET /v1/rides/{ride_id}/ledger
 *   200 {"ride_id":"...","entries":[{"id":1,"kind":"quote_hold","source_event_id":"...",
 *        "created_at":"...","postings":[{"account":"rider_receivable","amount":"5.85"},
 *                                        {"account":"fare_hold","amount":"-5.85"}]}]}
 *   404 {"error":"ledger not found"}
 * </pre>
 *
 * Amounts are strings with two decimals so no client turns them into floating point by accident.
 */
@RestController
public class LedgerController {

    private final LedgerRepository ledger;

    public LedgerController(LedgerRepository ledger) {
        this.ledger = ledger;
    }

    @GetMapping("/v1/rides/{ride_id}/ledger")
    public ResponseEntity<?> ledger(@PathVariable("ride_id") String rideId) {
        List<StoredJournalEntry> entries = ledger.findByRideId(rideId);
        if (entries.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "ledger not found"));
        }
        return ResponseEntity.ok(new LedgerResponse(rideId, entries.stream().map(LedgerController::view).toList()));
    }

    private static JournalEntryView view(StoredJournalEntry stored) {
        return new JournalEntryView(
                stored.id(),
                stored.entry().kind().code(),
                stored.entry().sourceEventId(),
                stored.createdAt(),
                stored.entry().postings().stream().map(LedgerController::view).toList());
    }

    private static PostingView view(Posting posting) {
        return new PostingView(posting.account().code(), posting.amount().toString());
    }

    record LedgerResponse(
            @JsonProperty("ride_id") String rideId,
            @JsonProperty("entries") List<JournalEntryView> entries) {
    }

    record JournalEntryView(
            @JsonProperty("id") long id,
            @JsonProperty("kind") String kind,
            @JsonProperty("source_event_id") String sourceEventId,
            @JsonProperty("created_at") Instant createdAt,
            @JsonProperty("postings") List<PostingView> postings) {
    }

    record PostingView(
            @JsonProperty("account") String account,
            @JsonProperty("amount") String amount) {
    }
}
