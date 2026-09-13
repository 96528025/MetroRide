//go:build fareintegration

// Package fareintegration drives one ride through the running Compose stack with the
// fare profile enabled and checks every hand-off between the Go services and the Java
// fare-service: rider-service accepts the ride, dispatch assigns it, fare-service holds
// the quote, rider-service completes the ride, fare-service reverses the hold and
// settles it, and the fare relay publishes fare_settled. Business actions go through
// the public HTTP APIs only; PostgreSQL and Redis are read to check results, never
// written to skip a step. Run it through scripts/fare-e2e-test.sh, which starts an
// isolated stack and passes the driver share it configured on fare-service.
package fareintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metroride/metroride/shared/pkg/events"
	"github.com/redis/go-redis/v9"
)

const (
	pollInterval   = 500 * time.Millisecond
	requestTimeout = 5 * time.Second
	// stageTimeout bounds each wait (assignment, hold, settlement, publication). The
	// assignment and completion events cross a 250ms outbox relay and a consumer that
	// reads with a 5s block, so a healthy stack finishes each stage in a few seconds.
	stageTimeout = 45 * time.Second

	riderServicePort = "8080"
	fareServicePort  = "8087"

	kindQuoteHold    = "quote_hold"
	kindHoldReversal = "hold_reversal"
	kindSettlement   = "settlement"

	accountRiderReceivable = "rider_receivable"
	accountFareHold        = "fare_hold"
	accountDriverPayable   = "driver_payable"
	accountPlatformRevenue = "platform_revenue"

	// driverShareEnv carries the share fare-service was started with. The script sets
	// the same value on the container and here, so the test checks the configured
	// split and not two defaults that happen to agree.
	driverShareEnv = "FARE_DRIVER_SHARE"
)

var amountPattern = regexp.MustCompile(`^-?[0-9]+\.[0-9]{2}$`)

// TestRideToFareSettlement is the whole chain, in the order the system needs it. Each
// stage records what it observed so a failure names the stage, the IDs involved and the
// last state seen instead of a bare timeout.
func TestRideToFareSettlement(t *testing.T) {
	share := configuredDriverShare(t)

	run := &rideRun{t: t, driverShare: share}
	db := openDB(t)
	defer db.Close()
	rdb := openRedis(t)
	defer func() { _ = rdb.Close() }()

	// Stage 1: the services this chain depends on are ready, then the ride is created.
	run.stage("ready")
	for _, ready := range []struct{ name, port string }{
		{"rider-service", riderServicePort},
		{"driver-service", "8081"},
		{"dispatch-service", "8082"},
		{"routing-service", "8083"},
		{"fare-service", fareServicePort},
	} {
		run.waitReady(ready.name, ready.port)
	}

	run.stage("create ride")
	run.riderID = "fare-e2e-" + uuid.NewString()
	created := run.createRide(createRideRequest{
		RiderID:    run.riderID,
		PickupLat:  37.775,
		PickupLng:  -122.419,
		DropoffLat: 37.789,
		DropoffLng: -122.401,
	})
	if _, err := uuid.Parse(created.RideID); err != nil {
		run.fatalf("create ride returned ride_id %q, want a UUID: %v", created.RideID, err)
	}
	if created.Status != "requested" {
		run.fatalf("create ride returned status %q, want requested", created.Status)
	}
	run.rideID = created.RideID

	// Stage 2: dispatch assigns the ride. The assignment row and the ride_assigned
	// envelope are read to learn which driver and assignment this run produced.
	run.stage("wait for assignment")
	ride := run.waitForAssignedRide()
	if ride.RiderID != run.riderID {
		run.fatalf("assigned ride carries rider_id %q, want %q", ride.RiderID, run.riderID)
	}
	if ride.DriverID == nil || *ride.DriverID == "" {
		run.fatalf("assigned ride has no driver_id: %+v", ride)
	}
	run.driverID = *ride.DriverID
	assignment := run.readAssignment(db)
	if assignment.DriverID != run.driverID {
		run.fatalf("ride_assignments row names driver %q, ride names %q", assignment.DriverID, run.driverID)
	}
	run.assignmentID = assignment.ID
	assigned := run.waitForRideAssignedEnvelope(rdb)
	run.assignmentEventID = assigned.ID
	assignedPayload, err := events.DecodePayload[events.RideAssigned](assigned)
	if err != nil {
		run.fatalf("decode ride_assigned payload: %v", err)
	}
	if assignedPayload.AssignmentID != run.assignmentID || assignedPayload.DriverID != run.driverID || assignedPayload.RiderID != run.riderID {
		run.fatalf("ride_assigned payload %+v does not match assignment %+v", assignedPayload, assignment)
	}

	// Stage 3: fare-service holds the quote. The hold must be observed before the ride
	// is completed, so the settlement below is provably a reaction to an existing hold.
	run.stage("wait for quote_hold")
	ledger := run.waitForLedger(func(l ledgerResponse) bool { return len(l.entriesOfKind(kindQuoteHold)) == 1 })
	if len(ledger.Entries) != 1 {
		run.fatalf("ledger before completion has %d entries, want only the quote_hold: %s", len(ledger.Entries), run.lastLedger)
	}
	hold := ledger.entriesOfKind(kindQuoteHold)[0]
	if hold.SourceEventID != run.assignmentEventID {
		run.fatalf("quote_hold source_event_id = %q, want the ride_assigned envelope %q", hold.SourceEventID, run.assignmentEventID)
	}
	holdPostings := run.balancedPostings(hold)
	quote := holdPostings[accountRiderReceivable]
	if quote <= 0 {
		run.fatalf("quote_hold holds %s for rider_receivable, want a positive amount", formatCents(quote))
	}
	if holdPostings[accountFareHold] != -quote || len(holdPostings) != 2 {
		run.fatalf("quote_hold postings %v, want rider_receivable +quote and fare_hold -quote only", hold.Postings)
	}
	run.quote = quote

	// Stage 4: the ride is completed through rider-service.
	run.stage("complete ride")
	code, completion := run.completeRide()
	if code != http.StatusAccepted {
		run.fatalf("complete ride: status %d, body %+v; want 202", code, completion)
	}
	if completion.RideID != run.rideID || completion.Status != "completed" || completion.EventID == "" {
		run.fatalf("complete ride: body %+v; want ride_id, status completed and an event_id", completion)
	}
	if _, err := uuid.Parse(completion.EventID); err != nil {
		run.fatalf("complete ride returned event_id %q, want a UUID: %v", completion.EventID, err)
	}
	run.completionEventID = completion.EventID

	// Stage 5: fare-service reverses the hold and settles by the held quote.
	run.stage("wait for settlement")
	ledger = run.waitForLedger(func(l ledgerResponse) bool {
		return len(l.entriesOfKind(kindHoldReversal)) >= 1 && len(l.entriesOfKind(kindSettlement)) >= 1
	})
	run.checkSettledLedger(ledger)
	settledEntryIDs := ledger.entryIDs()

	// Stage 6: the fare relay publishes fare_settled and marks its outbox row.
	run.stage("wait for fare_settled")
	settled := run.waitForFareSettled(rdb)
	run.checkFareSettled(settled)
	run.checkFareOutboxRow(db, settled)

	// Stage 7: a second completion is refused and changes nothing.
	run.stage("second completion")
	code, again := run.completeRide()
	if code != http.StatusConflict || again.Error != "ride is completed" {
		run.fatalf("second completion: status %d, body %+v; want 409 ride is completed", code, again)
	}
	after := run.readLedger()
	if !reflect.DeepEqual(after.entryIDs(), settledEntryIDs) {
		run.fatalf("ledger changed after the refused completion: entries %v, want %v", after.entryIDs(), settledEntryIDs)
	}
	if ids := run.distinctCompletionEventIDs(rdb); !reflect.DeepEqual(ids, []string{run.completionEventID}) {
		run.fatalf("ride_completed envelope ids on %s = %v, want only %s", events.StreamRideCompletions, ids, run.completionEventID)
	}
	if n := run.countGoOutboxRows(db, events.TypeRideCompleted); n != 1 {
		run.fatalf("ride_completed rows in event_outbox = %d, want 1", n)
	}
	run.checkFareOutboxRow(db, settled)
	if ids := run.distinctFareSettledIDs(rdb); !reflect.DeepEqual(ids, []string{settled.ID}) {
		run.fatalf("fare_settled envelope ids on %s = %v, want only %s", events.StreamRideFares, ids, settled.ID)
	}

	t.Logf("ride %s: rider %s, driver %s, assignment %s, quote %s, driver %s, platform %s (share %s), settlement event %s, fare_settled %s",
		run.rideID, run.riderID, run.driverID, run.assignmentID, formatCents(run.quote),
		formatCents(run.driverAmount), formatCents(run.platformAmount), share.RatString(), run.completionEventID, settled.ID)
}

// ---------------------------------------------------------------------------
// Ledger checks

func (r *rideRun) checkSettledLedger(ledger ledgerResponse) {
	holds := ledger.entriesOfKind(kindQuoteHold)
	reversals := ledger.entriesOfKind(kindHoldReversal)
	settlements := ledger.entriesOfKind(kindSettlement)
	if len(holds) != 1 || len(reversals) != 1 || len(settlements) != 1 || len(ledger.Entries) != 3 {
		r.fatalf("settled ledger has %d quote_hold, %d hold_reversal, %d settlement (%d entries); want exactly one of each",
			len(holds), len(reversals), len(settlements), len(ledger.Entries))
	}
	hold, reversal, settlement := holds[0], reversals[0], settlements[0]

	holdPostings := r.balancedPostings(hold)
	reversalPostings := r.balancedPostings(reversal)
	settlementPostings := r.balancedPostings(settlement)

	if holdPostings[accountRiderReceivable] != r.quote {
		r.fatalf("quote_hold changed after completion: rider_receivable %s, was %s", formatCents(holdPostings[accountRiderReceivable]), formatCents(r.quote))
	}
	if len(reversalPostings) != len(holdPostings) {
		r.fatalf("hold_reversal has %d postings, hold has %d", len(reversalPostings), len(holdPostings))
	}
	for account, amount := range holdPostings {
		if reversalPostings[account] != -amount {
			r.fatalf("hold_reversal posting %s = %s, want %s (the hold negated)", account, formatCents(reversalPostings[account]), formatCents(-amount))
		}
	}
	if reversal.SourceEventID != r.completionEventID || settlement.SourceEventID != r.completionEventID {
		r.fatalf("hold_reversal source_event_id %q and settlement source_event_id %q, want the ride_completed envelope %q",
			reversal.SourceEventID, settlement.SourceEventID, r.completionEventID)
	}
	if hold.SourceEventID == r.completionEventID {
		r.fatalf("quote_hold source_event_id %q is the completion event; want the assignment event", hold.SourceEventID)
	}

	// Settlement by quote: the rider is debited the held quote, the driver is credited
	// quote * share rounded half up once, and the platform is credited the remainder.
	// A zero share is omitted rather than posted.
	if settlementPostings[accountRiderReceivable] != r.quote {
		r.fatalf("settlement debits rider_receivable %s, want the held quote %s", formatCents(settlementPostings[accountRiderReceivable]), formatCents(r.quote))
	}
	wantDriver := roundHalfUp(new(big.Rat).Mul(big.NewRat(r.quote, 1), r.driverShare))
	wantPlatform := r.quote - wantDriver
	r.driverAmount, r.platformAmount = wantDriver, wantPlatform
	wantPostings := map[string]int64{accountRiderReceivable: r.quote}
	if wantDriver != 0 {
		wantPostings[accountDriverPayable] = -wantDriver
	}
	if wantPlatform != 0 {
		wantPostings[accountPlatformRevenue] = -wantPlatform
	}
	if !reflect.DeepEqual(settlementPostings, wantPostings) {
		r.fatalf("settlement postings %v, want %v (quote %s, driver share %s)", describe(settlementPostings), describe(wantPostings), formatCents(r.quote), r.driverShare.RatString())
	}
}

// balancedPostings checks that every amount is a two-decimal string, that no account
// appears twice, that no posting is zero and that the entry sums to zero, and returns
// the postings in integer cents by account.
func (r *rideRun) balancedPostings(entry ledgerEntry) map[string]int64 {
	postings := map[string]int64{}
	var sum int64
	if len(entry.Postings) == 0 {
		r.fatalf("%s entry %d has no postings", entry.Kind, entry.ID)
	}
	for _, posting := range entry.Postings {
		amount, err := parseCents(posting.Amount)
		if err != nil {
			r.fatalf("%s entry %d posting %s: %v", entry.Kind, entry.ID, posting.Account, err)
		}
		if amount == 0 {
			r.fatalf("%s entry %d posts zero to %s", entry.Kind, entry.ID, posting.Account)
		}
		if _, seen := postings[posting.Account]; seen {
			r.fatalf("%s entry %d posts to %s twice", entry.Kind, entry.ID, posting.Account)
		}
		postings[posting.Account] = amount
		sum += amount
	}
	if sum != 0 {
		r.fatalf("%s entry %d postings sum to %s, want 0.00: %v", entry.Kind, entry.ID, formatCents(sum), entry.Postings)
	}
	return postings
}

// ---------------------------------------------------------------------------
// fare_settled checks

func (r *rideRun) checkFareSettled(envelope events.Envelope) {
	if envelope.Type != events.TypeFareSettled || envelope.Source != "fare-service" || envelope.CorrelationID != r.rideID {
		r.fatalf("fare_settled envelope type=%q source=%q correlation_id=%q; want fare_settled from fare-service correlated to %s",
			envelope.Type, envelope.Source, envelope.CorrelationID, r.rideID)
	}
	if envelope.OccurredAt.IsZero() {
		r.fatalf("fare_settled envelope has no occurred_at")
	}

	// The contract says amounts and the share are JSON strings. Check the raw tokens
	// before decoding, because decoding into strings would also accept numbers.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(envelope.Payload, &raw); err != nil {
		r.fatalf("fare_settled payload is not an object: %v", err)
	}
	for _, field := range []string{"quote", "driver_amount", "platform_amount", "driver_share", "settled_at"} {
		token, ok := raw[field]
		if !ok || len(token) < 2 || token[0] != '"' {
			r.fatalf("fare_settled payload field %s = %s, want a JSON string", field, string(token))
		}
	}

	payload, err := events.DecodePayload[events.FareSettled](envelope)
	if err != nil {
		r.fatalf("decode fare_settled payload: %v", err)
	}
	if payload.RideID != r.rideID || payload.RiderID != r.riderID || payload.DriverID != r.driverID || payload.AssignmentID != r.assignmentID {
		r.fatalf("fare_settled ids %+v; want ride %s rider %s driver %s assignment %s", payload, r.rideID, r.riderID, r.driverID, r.assignmentID)
	}
	if payload.SettlementEventID != r.completionEventID {
		r.fatalf("fare_settled settlement_event_id %q, want the ride_completed envelope %q", payload.SettlementEventID, r.completionEventID)
	}
	for _, check := range []struct {
		field string
		got   string
		want  int64
	}{
		{"quote", payload.Quote, r.quote},
		{"driver_amount", payload.DriverAmount, r.driverAmount},
		{"platform_amount", payload.PlatformAmount, r.platformAmount},
	} {
		got, err := parseCents(check.got)
		if err != nil {
			r.fatalf("fare_settled %s: %v", check.field, err)
		}
		if got != check.want {
			r.fatalf("fare_settled %s = %s, ledger says %s", check.field, check.got, formatCents(check.want))
		}
	}
	share, ok := new(big.Rat).SetString(payload.DriverShare)
	if !ok || share.Cmp(r.driverShare) != 0 {
		r.fatalf("fare_settled driver_share = %q, fare-service was configured with %s", payload.DriverShare, r.driverShare.RatString())
	}
	if _, err := time.Parse(time.RFC3339Nano, payload.SettledAt); err != nil {
		r.fatalf("fare_settled settled_at %q is not RFC 3339: %v", payload.SettledAt, err)
	}
}

// checkFareOutboxRow reads fare.event_outbox for this ride: exactly one fare_settled
// row, marked published, whose stored envelope is the one seen on the stream. jsonb does
// not keep key order, so the two are compared as decoded documents.
func (r *rideRun) checkFareOutboxRow(db *pgxpool.Pool, published events.Envelope) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	rows, err := db.Query(ctx, `
		select id, stream, published_at, envelope
		from fare.event_outbox
		where aggregate_id = $1 and event_type = $2
	`, r.rideID, events.TypeFareSettled)
	if err != nil {
		r.fatalf("query fare.event_outbox: %v", err)
	}
	type outboxRow struct {
		ID          string
		Stream      string
		PublishedAt *time.Time
		Envelope    []byte
	}
	found, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outboxRow, error) {
		var out outboxRow
		err := row.Scan(&out.ID, &out.Stream, &out.PublishedAt, &out.Envelope)
		return out, err
	})
	if err != nil {
		r.fatalf("read fare.event_outbox rows: %v", err)
	}
	if len(found) != 1 {
		r.fatalf("fare.event_outbox has %d fare_settled rows for the ride, want 1: %+v", len(found), found)
	}
	row := found[0]
	if row.ID != published.ID || row.Stream != events.StreamRideFares {
		r.fatalf("fare.event_outbox row id=%s stream=%s, want the published envelope %s on %s", row.ID, row.Stream, published.ID, events.StreamRideFares)
	}
	if row.PublishedAt == nil {
		r.fatalf("fare.event_outbox row %s has no published_at although the envelope is on the stream", row.ID)
	}
	stored, err := decodeDocument(row.Envelope)
	if err != nil {
		r.fatalf("decode stored envelope: %v", err)
	}
	wire, err := json.Marshal(published)
	if err != nil {
		r.fatalf("re-encode published envelope: %v", err)
	}
	onWire, err := decodeDocument(wire)
	if err != nil {
		r.fatalf("decode published envelope: %v", err)
	}
	if !reflect.DeepEqual(stored, onWire) {
		r.fatalf("stored envelope differs from the published one:\n stored:    %s\n published: %s", string(row.Envelope), string(wire))
	}
}

// ---------------------------------------------------------------------------
// Waits

func (r *rideRun) waitReady(name, port string) {
	deadline := time.Now().Add(stageTimeout)
	for {
		status, body := r.get(fmt.Sprintf("%s/readyz", baseURL(port)), true)
		r.lastObserved = fmt.Sprintf("%s /readyz -> %d %s", name, status, body)
		if status == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			r.fatalf("%s did not become ready within %s", name, stageTimeout)
		}
		time.Sleep(pollInterval)
	}
}

func (r *rideRun) waitForAssignedRide() rideResponse {
	deadline := time.Now().Add(stageTimeout)
	for {
		ride := r.getRide()
		r.lastObserved = fmt.Sprintf("ride %+v", ride)
		if ride.Status == "assigned" {
			return ride
		}
		if time.Now().After(deadline) {
			r.fatalf("ride was not assigned within %s", stageTimeout)
		}
		time.Sleep(pollInterval)
	}
}

func (r *rideRun) waitForRideAssignedEnvelope(rdb *redis.Client) events.Envelope {
	deadline := time.Now().Add(stageTimeout)
	for {
		found := r.envelopesForRide(rdb, events.StreamRideAssignments, events.TypeRideAssigned)
		ids := distinctIDs(found)
		r.lastObserved = fmt.Sprintf("ride_assigned envelope ids on %s: %v", events.StreamRideAssignments, ids)
		if len(ids) == 1 {
			return found[0]
		}
		if len(ids) > 1 {
			r.fatalf("more than one distinct ride_assigned envelope for the ride: %v", ids)
		}
		if time.Now().After(deadline) {
			r.fatalf("ride_assigned envelope did not appear on %s within %s", events.StreamRideAssignments, stageTimeout)
		}
		time.Sleep(pollInterval)
	}
}

// waitForLedger polls the ledger endpoint until ready(ledger) holds. A 404 means
// fare-service has not recorded anything for the ride yet and is treated as "not yet".
func (r *rideRun) waitForLedger(ready func(ledgerResponse) bool) ledgerResponse {
	deadline := time.Now().Add(stageTimeout)
	for {
		status, body := r.get(fmt.Sprintf("%s/v1/rides/%s/ledger", baseURL(fareServicePort), r.rideID), false)
		r.lastLedger = fmt.Sprintf("%d %s", status, body)
		r.lastObserved = "ledger " + r.lastLedger
		switch status {
		case http.StatusOK:
			var ledger ledgerResponse
			if err := json.Unmarshal([]byte(body), &ledger); err != nil {
				r.fatalf("decode ledger response %q: %v", body, err)
			}
			if ledger.RideID != r.rideID {
				r.fatalf("ledger response is for ride %q, asked for %q", ledger.RideID, r.rideID)
			}
			if ready(ledger) {
				return ledger
			}
		case http.StatusNotFound:
		default:
			r.fatalf("ledger endpoint answered %d: %s", status, body)
		}
		if time.Now().After(deadline) {
			r.fatalf("ledger did not reach the expected state within %s", stageTimeout)
		}
		time.Sleep(pollInterval)
	}
}

func (r *rideRun) readLedger() ledgerResponse {
	return r.waitForLedger(func(ledgerResponse) bool { return true })
}

// waitForFareSettled waits until at least one fare_settled envelope for the ride is on
// events.ride.fares. The relay is at-least-once, so several copies are accepted, but
// they must all be the same envelope: one ID and one content.
func (r *rideRun) waitForFareSettled(rdb *redis.Client) events.Envelope {
	deadline := time.Now().Add(stageTimeout)
	for {
		found := r.envelopesForRide(rdb, events.StreamRideFares, events.TypeFareSettled)
		r.lastObserved = fmt.Sprintf("%d fare_settled entries on %s for the ride", len(found), events.StreamRideFares)
		if len(found) > 0 {
			r.requireIdenticalCopies(found)
			return found[0]
		}
		if time.Now().After(deadline) {
			r.fatalf("fare_settled did not appear on %s within %s", events.StreamRideFares, stageTimeout)
		}
		time.Sleep(pollInterval)
	}
}

func (r *rideRun) requireIdenticalCopies(copies []events.Envelope) {
	first, err := decodeDocument(mustMarshal(r, copies[0]))
	if err != nil {
		r.fatalf("decode fare_settled envelope: %v", err)
	}
	for _, other := range copies[1:] {
		doc, err := decodeDocument(mustMarshal(r, other))
		if err != nil {
			r.fatalf("decode fare_settled envelope: %v", err)
		}
		if other.ID != copies[0].ID || !reflect.DeepEqual(doc, first) {
			r.fatalf("fare_settled copies on %s differ: %+v vs %+v", events.StreamRideFares, copies[0], other)
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP

type createRideRequest struct {
	RiderID    string  `json:"rider_id"`
	PickupLat  float64 `json:"pickup_lat"`
	PickupLng  float64 `json:"pickup_lng"`
	DropoffLat float64 `json:"dropoff_lat"`
	DropoffLng float64 `json:"dropoff_lng"`
}

type createRideResponse struct {
	RideID string `json:"ride_id"`
	Status string `json:"status"`
}

type rideResponse struct {
	ID       string  `json:"id"`
	RiderID  string  `json:"rider_id"`
	DriverID *string `json:"driver_id"`
	Status   string  `json:"status"`
}

type completeRideResponse struct {
	RideID  string `json:"ride_id"`
	Status  string `json:"status"`
	EventID string `json:"event_id"`
	Error   string `json:"error"`
}

type ledgerResponse struct {
	RideID  string        `json:"ride_id"`
	Entries []ledgerEntry `json:"entries"`
}

type ledgerEntry struct {
	ID            int64     `json:"id"`
	Kind          string    `json:"kind"`
	SourceEventID string    `json:"source_event_id"`
	CreatedAt     string    `json:"created_at"`
	Postings      []posting `json:"postings"`
}

type posting struct {
	Account string `json:"account"`
	Amount  string `json:"amount"`
}

func (l ledgerResponse) entriesOfKind(kind string) []ledgerEntry {
	var out []ledgerEntry
	for _, entry := range l.Entries {
		if entry.Kind == kind {
			out = append(out, entry)
		}
	}
	return out
}

func (l ledgerResponse) entryIDs() []int64 {
	ids := make([]int64, 0, len(l.Entries))
	for _, entry := range l.Entries {
		ids = append(ids, entry.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

var httpClient = &http.Client{Timeout: requestTimeout}

func (r *rideRun) createRide(req createRideRequest) createRideResponse {
	body, err := json.Marshal(req)
	if err != nil {
		r.fatalf("marshal create ride request: %v", err)
	}
	status, raw := r.do(http.MethodPost, baseURL(riderServicePort)+"/v1/rides", body)
	if status != http.StatusAccepted {
		r.fatalf("create ride: status %d, body %s; want 202", status, raw)
	}
	var out createRideResponse
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		r.fatalf("decode create ride response %q: %v", raw, err)
	}
	if out.RideID == "" {
		r.fatalf("create ride response %s has no ride_id", raw)
	}
	return out
}

func (r *rideRun) getRide() rideResponse {
	status, raw := r.get(baseURL(riderServicePort)+"/v1/rides/"+r.rideID, false)
	if status != http.StatusOK {
		r.fatalf("get ride: status %d, body %s", status, raw)
	}
	var out rideResponse
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		r.fatalf("decode get ride response %q: %v", raw, err)
	}
	if out.ID != r.rideID {
		r.fatalf("get ride returned ride %q, asked for %q", out.ID, r.rideID)
	}
	return out
}

func (r *rideRun) completeRide() (int, completeRideResponse) {
	status, raw := r.do(http.MethodPost, baseURL(riderServicePort)+"/v1/rides/"+r.rideID+"/complete", nil)
	var out completeRideResponse
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		r.fatalf("decode complete ride response %q (status %d): %v", raw, status, err)
	}
	r.lastObserved = fmt.Sprintf("complete -> %d %s", status, raw)
	return status, out
}

// get performs a GET. With tolerateErrors, a transport failure is reported as status 0
// instead of failing the test, for readiness polling of a service that is still starting.
func (r *rideRun) get(url string, tolerateErrors bool) (int, string) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		r.fatalf("build GET %s: %v", url, err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		if tolerateErrors {
			return 0, err.Error()
		}
		r.fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		r.fatalf("read GET %s response: %v", url, err)
	}
	return resp.StatusCode, strings.TrimSpace(string(raw))
}

func (r *rideRun) do(method, url string, body []byte) (int, string) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		r.fatalf("build %s %s: %v", method, url, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		r.fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		r.fatalf("read %s %s response: %v", method, url, err)
	}
	return resp.StatusCode, strings.TrimSpace(string(raw))
}

// ---------------------------------------------------------------------------
// Read-only PostgreSQL and Redis access

type assignmentRow struct {
	ID         string
	DriverID   string
	DistanceKM float64
	ETASeconds int
}

// readAssignment returns the single ride_assignments row for the ride. Dispatch inserts
// it in the transaction that moves the ride to assigned, so it exists once the ride does.
func (r *rideRun) readAssignment(db *pgxpool.Pool) assignmentRow {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	rows, err := db.Query(ctx, `
		select id, driver_id, distance_km, eta_seconds from ride_assignments where ride_id = $1
	`, r.rideID)
	if err != nil {
		r.fatalf("query ride_assignments: %v", err)
	}
	found, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (assignmentRow, error) {
		var out assignmentRow
		err := row.Scan(&out.ID, &out.DriverID, &out.DistanceKM, &out.ETASeconds)
		return out, err
	})
	if err != nil {
		r.fatalf("read ride_assignments rows: %v", err)
	}
	if len(found) != 1 {
		r.fatalf("ride_assignments has %d rows for the ride, want 1: %+v", len(found), found)
	}
	return found[0]
}

func (r *rideRun) countGoOutboxRows(db *pgxpool.Pool, eventType string) int {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	var count int
	if err := db.QueryRow(ctx, `
		select count(*) from event_outbox where aggregate_id = $1 and event_type = $2
	`, r.rideID, eventType).Scan(&count); err != nil {
		r.fatalf("count event_outbox rows: %v", err)
	}
	return count
}

// envelopesForRide returns every envelope of the given type on the stream whose
// correlation_id is this ride, in stream order.
func (r *rideRun) envelopesForRide(rdb *redis.Client, stream, eventType string) []events.Envelope {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	messages, err := rdb.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		r.fatalf("read %s: %v", stream, err)
	}
	var found []events.Envelope
	for _, message := range messages {
		envelope, err := events.DecodeEnvelope(message)
		if err != nil {
			r.fatalf("decode entry %s on %s: %v", message.ID, stream, err)
		}
		if envelope.Type == eventType && envelope.CorrelationID == r.rideID {
			found = append(found, envelope)
		}
	}
	return found
}

func (r *rideRun) distinctCompletionEventIDs(rdb *redis.Client) []string {
	return distinctIDs(r.envelopesForRide(rdb, events.StreamRideCompletions, events.TypeRideCompleted))
}

func (r *rideRun) distinctFareSettledIDs(rdb *redis.Client) []string {
	return distinctIDs(r.envelopesForRide(rdb, events.StreamRideFares, events.TypeFareSettled))
}

func distinctIDs(envelopes []events.Envelope) []string {
	seen := map[string]bool{}
	var ids []string
	for _, envelope := range envelopes {
		if !seen[envelope.ID] {
			seen[envelope.ID] = true
			ids = append(ids, envelope.ID)
		}
	}
	return ids
}

func openDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	dsn := getenv("INTEGRATION_POSTGRES_DSN", "postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable")
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("ping postgres at %s: %v", dsn, err)
	}
	return db
}

func openRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:         getenv("INTEGRATION_REDIS_ADDR", "localhost:6379"),
		DialTimeout:  requestTimeout,
		ReadTimeout:  requestTimeout,
		WriteTimeout: requestTimeout,
	})
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return rdb
}

func baseURL(port string) string {
	return fmt.Sprintf("http://%s:%s", getenv("INTEGRATION_HOST", "localhost"), port)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// configuredDriverShare reads the share fare-service was started with. There is no
// default on purpose: a default here would only ever agree with the service's own
// default, and the test would then prove nothing about the configured split.
func configuredDriverShare(t *testing.T) *big.Rat {
	t.Helper()
	value := os.Getenv(driverShareEnv)
	if value == "" {
		t.Fatalf("%s is not set; run this test through scripts/fare-e2e-test.sh, which starts fare-service with the same value", driverShareEnv)
	}
	share, ok := new(big.Rat).SetString(value)
	if !ok || share.Sign() < 0 || share.Cmp(big.NewRat(1, 1)) > 0 {
		t.Fatalf("%s=%q is not a decimal between 0 and 1", driverShareEnv, value)
	}
	return share
}

// ---------------------------------------------------------------------------
// Money: integer cents, exact rounding

func parseCents(amount string) (int64, error) {
	if !amountPattern.MatchString(amount) {
		return 0, fmt.Errorf("amount %q is not a two-decimal string", amount)
	}
	negative := strings.HasPrefix(amount, "-")
	digits := strings.TrimPrefix(amount, "-")
	whole, fraction, _ := strings.Cut(digits, ".")
	var cents int64
	for _, c := range whole + fraction {
		cents = cents*10 + int64(c-'0')
	}
	if negative {
		cents = -cents
	}
	return cents, nil
}

func formatCents(cents int64) string {
	sign := ""
	if cents < 0 {
		sign, cents = "-", -cents
	}
	return fmt.Sprintf("%s%d.%02d", sign, cents/100, cents%100)
}

// roundHalfUp rounds a non-negative exact value to the nearest integer, halves up,
// the same rule Money applies once to the driver's share.
func roundHalfUp(value *big.Rat) int64 {
	if value.Sign() < 0 {
		panic("roundHalfUp expects a non-negative value")
	}
	shifted := new(big.Rat).Add(value, big.NewRat(1, 2))
	floor := new(big.Int).Quo(shifted.Num(), shifted.Denom())
	return floor.Int64()
}

func describe(postings map[string]int64) string {
	accounts := make([]string, 0, len(postings))
	for account := range postings {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)
	parts := make([]string, 0, len(accounts))
	for _, account := range accounts {
		parts = append(parts, account+"="+formatCents(postings[account]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func decodeDocument(raw []byte) (any, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func mustMarshal(r *rideRun, envelope events.Envelope) []byte {
	raw, err := json.Marshal(envelope)
	if err != nil {
		r.fatalf("encode envelope %s: %v", envelope.ID, err)
	}
	return raw
}

// ---------------------------------------------------------------------------
// Run state and failure reporting

type rideRun struct {
	t           *testing.T
	driverShare *big.Rat

	currentStage      string
	riderID           string
	rideID            string
	driverID          string
	assignmentID      string
	assignmentEventID string
	completionEventID string
	quote             int64
	driverAmount      int64
	platformAmount    int64

	lastLedger   string
	lastObserved string
}

func (r *rideRun) stage(name string) {
	r.currentStage = name
	r.lastObserved = ""
	r.t.Logf("stage: %s", name)
}

// fatalf fails the test with the stage, every ID the run has produced so far and the
// last state observed, so a CI log says where the chain stopped without the reader
// having to reconstruct it from service logs.
func (r *rideRun) fatalf(format string, args ...any) {
	r.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "stage %q failed: %s\n", r.currentStage, fmt.Sprintf(format, args...))
	fmt.Fprintf(&b, "  ride_id=%s rider_id=%s driver_id=%s assignment_id=%s\n", r.rideID, r.riderID, r.driverID, r.assignmentID)
	fmt.Fprintf(&b, "  ride_assigned_event_id=%s ride_completed_event_id=%s quote=%s\n", r.assignmentEventID, r.completionEventID, formatCents(r.quote))
	if r.lastObserved != "" {
		fmt.Fprintf(&b, "  last observed: %s\n", r.lastObserved)
	}
	if r.lastLedger != "" && !strings.HasPrefix(r.lastObserved, "ledger ") {
		fmt.Fprintf(&b, "  last ledger: %s\n", r.lastLedger)
	}
	r.t.Fatal(strings.TrimRight(b.String(), "\n"))
}
