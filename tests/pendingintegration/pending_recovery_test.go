//go:build pendingintegration

package pendingintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

// This test deliberately stops only the explicitly named disposable Compose project.
func TestDispatchRestartReclaimsAnAbandonedDeliveryAndAnAlreadyAssignedReplay(t *testing.T) {
	project := os.Getenv("PENDING_RECOVERY_PROJECT")
	if project == "" {
		t.Fatal("set PENDING_RECOVERY_PROJECT to a disposable Compose project")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	compose := func(args ...string) {
		t.Helper()
		all := append([]string{"compose", "-p", project, "-f", "docker-compose.yml", "-f", "tests/routingfixture/compose.yml"}, args...)
		cmd := exec.CommandContext(ctx, "docker", all...)
		cmd.Dir = "../.."
		if output, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("compose %v: %v %s", args, e, output)
		}
	}
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	db, e := pgxpool.New(ctx, "postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	client := http.Client{Timeout: 5 * time.Second}
	rideID := ""
	defer func() {
		cmd := exec.Command("docker", "compose", "-p", project, "-f", "docker-compose.yml", "-f", "tests/routingfixture/compose.yml", "up", "-d", "--no-deps", "dispatch-service")
		cmd.Dir = "../.."
		cmd.Run()
		if rideID != "" {
			req, _ := http.NewRequest("POST", "http://localhost:8080/v1/rides/"+rideID+"/cancel", nil)
			if resp, e := client.Do(req); e == nil {
				resp.Body.Close()
			}
		}
	}()
	compose("stop", "-t", "5", "dispatch-service")
	body := []byte(`{"rider_id":"pending-restart","pickup_lat":37.775,"pickup_lng":-122.419,"dropoff_lat":37.789,"dropoff_lng":-122.401}`)
	resp, e := client.Post("http://localhost:8080/v1/rides", "application/json", bytes.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	var created struct {
		RideID  string `json:"ride_id"`
		EventID string `json:"event_id"`
	}
	e = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if e != nil || resp.StatusCode != 202 {
		t.Fatalf("create: %v status %d", e, resp.StatusCode)
	}
	rideID = created.RideID
	stream, group := "events.ride.requests", "dispatch-service"
	// Take the real outbox delivery under an abandoned consumer identity.
	var original redis.XMessage
	for original.ID == "" {
		rows, e := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "abandoned-" + uuid.NewString(), Streams: []string{stream, ">"}, Count: 1, Block: time.Second}).Result()
		if e != nil && e != redis.Nil {
			t.Fatal(e)
		}
		for _, row := range rows {
			for _, m := range row.Messages {
				raw, _ := m.Values["event"].(string)
				var env struct {
					ID string `json:"id"`
				}
				json.Unmarshal([]byte(raw), &env)
				if env.ID != created.EventID {
					t.Fatalf("unexpected unrelated unread request %s; use a quiet disposable stack", m.ID)
				}
				original = m
			}
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	waitAssignedAndAck := func(messageID string) {
		t.Helper()
		end := time.Now().Add(35 * time.Second)
		for time.Now().Before(end) {
			var status string
			db.QueryRow(ctx, "select status from rides where id=$1", rideID).Scan(&status)
			p, e := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: group, Start: messageID, End: messageID, Count: 1}).Result()
			if e == nil && status == "assigned" && len(p) == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("abandoned request did not recover")
	}
	compose("up", "-d", "--no-deps", "dispatch-service")
	waitAssignedAndAck(original.ID)
	// Leave a replay pending after the durable assignment already exists.
	compose("stop", "-t", "5", "dispatch-service")
	replay, e := rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: original.Values}).Result()
	if e != nil {
		t.Fatal(e)
	}
	if e = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "abandoned-after-commit", Streams: []string{stream, ">"}, Count: 1, Block: time.Second}).Err(); e != nil {
		t.Fatal(e)
	}
	compose("up", "-d", "--no-deps", "dispatch-service")
	waitAssignedAndAck(replay)
	var assignments, reservations int
	if e = db.QueryRow(ctx, `select (select count(*) from ride_assignments where ride_id=$1),(select count(*) from driver_reservations where ride_id=$1)`, rideID).Scan(&assignments, &reservations); e != nil {
		t.Fatal(e)
	}
	if assignments != 1 || reservations != 1 {
		t.Fatal(fmt.Sprintf("after restart: %d assignments, %d reservations", assignments, reservations))
	}
}
