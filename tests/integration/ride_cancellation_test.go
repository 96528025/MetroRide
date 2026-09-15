//go:build integration

package integration

import (
	"context"
	"github.com/google/uuid"
	"net/http"
	"testing"
	"time"
)

func TestCompletionCancellationRaceReleasesExactlyOneReservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	created := createRide(t, ctx, createRideRequest{RiderID: "cancel-race-" + uuid.NewString(), PickupLat: 37.775, PickupLng: -122.419, DropoffLat: 37.789, DropoffLng: -122.401})
	waitForAssignedRide(t, ctx, created.RideID)
	results := make(chan int, 2)
	start := make(chan struct{})
	for _, action := range []string{"complete", "cancel"} {
		go func(action string) {
			<-start
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL("8080")+"/v1/rides/"+created.RideID+"/"+action, nil)
			resp, e := http.DefaultClient.Do(req)
			if e != nil {
				results <- 0
				return
			}
			defer resp.Body.Close()
			results <- resp.StatusCode
		}(action)
	}
	close(start)
	a, b := <-results, <-results
	if !((a == 202 && b == 409) || (a == 409 && b == 202)) {
		t.Fatalf("racing transitions=%d,%d", a, b)
	}
	db := openDB(t, ctx)
	defer db.Close()
	var reservations, events int
	if e := db.QueryRow(ctx, `select (select count(*) from driver_reservations where ride_id=$1),(select count(*) from event_outbox where aggregate_id=$2 and event_type in ('ride_completed','ride_cancelled'))`, created.RideID, created.RideID).Scan(&reservations, &events); e != nil {
		t.Fatal(e)
	}
	if reservations != 0 || events != 1 {
		t.Fatalf("remaining reservations=%d terminal events=%d", reservations, events)
	}
}
func TestCancelRequestedRideWithoutAssignment(t *testing.T) {
	// Construct a requested legacy row without publishing a request, so no dispatch timing can hide this case.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := openDB(t, ctx)
	defer db.Close()
	id := uuid.NewString()
	_, e := db.Exec(ctx, `insert into rides(id,rider_id,pickup_lat,pickup_lng,dropoff_lat,dropoff_lng,status,created_at,updated_at) values($1,'cancel-requested',37,-122,38,-123,'requested',now(),now())`, id)
	if e != nil {
		t.Fatal(e)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL("8080")+"/v1/rides/"+id+"/cancel", nil)
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("cancel requested status=%d", resp.StatusCode)
	}
	if status := rideStatus(t, ctx, db, id); status != "cancelled" {
		t.Fatalf("status=%s", status)
	}
}
