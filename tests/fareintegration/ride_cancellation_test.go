//go:build fareintegration

package fareintegration

import (
	"context"
	"github.com/google/uuid"
	"net/http"
	"testing"
	"time"
)

func TestRideCancellationReversesHoldWithoutSettlement(t *testing.T) {
	run := &rideRun{t: t}
	db := openDB(t)
	defer db.Close()
	run.riderID = "fare-cancel-" + uuid.NewString()
	created := run.createRide(createRideRequest{RiderID: run.riderID, PickupLat: 37.775, PickupLng: -122.419, DropoffLat: 37.789, DropoffLng: -122.401})
	run.rideID = created.RideID
	run.waitForAssignedRide()
	run.waitForLedger(func(l ledgerResponse) bool { return len(l.entriesOfKind(kindQuoteHold)) == 1 })
	status, raw := run.do(http.MethodPost, baseURL(riderServicePort)+"/v1/rides/"+run.rideID+"/cancel", nil)
	if status != 202 {
		run.fatalf("cancel status=%d body=%s", status, raw)
	}
	ledger := run.waitForLedger(func(l ledgerResponse) bool { return len(l.entriesOfKind("cancellation_reversal")) == 1 })
	if len(ledger.Entries) != 2 || len(ledger.entriesOfKind(kindSettlement)) != 0 {
		run.fatalf("cancellation ledger=%+v", ledger)
	}
	totals := map[string]int64{}
	for _, entry := range ledger.Entries {
		for account, amount := range run.balancedPostings(entry) {
			totals[account] += amount
		}
	}
	for account, total := range totals {
		if total != 0 {
			run.fatalf("cancellation left %d cents in %s", total, account)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int
	if e := db.QueryRow(ctx, `select count(*) from driver_reservations where ride_id=$1`, run.rideID).Scan(&count); e != nil || count != 0 {
		run.fatalf("reservation count=%d error=%v", count, e)
	}
	status, _ = run.do(http.MethodPost, baseURL(riderServicePort)+"/v1/rides/"+run.rideID+"/cancel", nil)
	if status != 409 {
		run.fatalf("repeat cancellation status=%d", status)
	}
}
