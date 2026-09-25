//go:build kafkaintegration

// Package kafkaintegration checks the optional Kafka telemetry path end to end against
// a running Compose stack with the `kafka` profile: the driver telemetry producer
// publishes location events keyed by driver_id, and analytics-service consumes them in
// its consumer group. Kafka is read through its own protocol (metadata, offsets, group
// state, the log itself); nothing is written to the topic by the test.
//
// Run it through scripts/kafka-e2e-test.sh. The test runs inside a container on the
// Compose network, reaches the broker as kafka:9092 (the only advertised listener),
// and stops/starts containers of the one disposable Compose project named in
// KAFKA_E2E_PROJECT through the Docker socket the script mounts.
package kafkaintegration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metroride/metroride/shared/pkg/events"
	sharedkafka "github.com/metroride/metroride/shared/pkg/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	consumerGroup = "metroride-analytics-service"
	// The producer publishes one event per driver every 10s, so every wait on new
	// data allows several publish rounds.
	stageTimeout   = 60 * time.Second
	pollInterval   = 500 * time.Millisecond
	requestTimeout = 5 * time.Second
)

type env struct {
	project      string
	broker       string
	analyticsURL string
	producerURL  string
	client       *kafkago.Client
	http         *http.Client
}

type logRecord struct {
	partition int
	offset    int64
	key       string
	event     sharedkafka.DriverLocationEvent
}

type analyticsDriver struct {
	DriverID  string    `json:"driver_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	Available bool      `json:"available"`
	Timestamp time.Time `json:"timestamp"`
}

type analyticsView struct {
	Topic         string            `json:"topic"`
	ConsumerGroup string            `json:"consumer_group"`
	TotalConsumed int64             `json:"total_consumed"`
	Drivers       []analyticsDriver `json:"drivers"`
}

func TestDriverLocationTelemetryThroughKafka(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The script publishes no host ports, so readiness is checked from here, on the
	// Compose network.
	e.waitForReady(ctx, t, "driver-kafka-producer", e.producerURL)
	e.waitForReady(ctx, t, "analytics-service", e.analyticsURL)

	partitions := e.topicPartitions(ctx, t)
	if len(partitions) != 3 {
		t.Fatalf("topic %s has %d partitions, want the 3 kafka-init creates", sharedkafka.DriverLocationTopic, len(partitions))
	}
	drivers := e.simulatedDrivers(ctx, t)
	t.Logf("topic %s, partitions %v, simulated drivers %v", sharedkafka.DriverLocationTopic, partitions, drivers)

	t.Run("events are keyed by driver and ordered per driver", func(t *testing.T) {
		// Two events per driver is the minimum that makes ordering checkable.
		records := e.waitForLog(ctx, t, partitions, func(records []logRecord) bool {
			perDriver := map[string]int{}
			for _, r := range records {
				perDriver[r.event.DriverID]++
			}
			for _, d := range drivers {
				if perDriver[d] < 2 {
					return false
				}
			}
			return true
		})
		checkKeyingAndOrder(t, records, drivers, len(partitions))
	})
	if t.Failed() {
		return
	}

	t.Run("consumer group totals, committed offsets and latest view match the log", func(t *testing.T) {
		e.dockerAction(ctx, t, "stop", "driver-kafka-producer")
		records := e.readLog(ctx, t, partitions)
		e.waitForDrained(ctx, t, partitions, int64(len(records)))
		e.checkLatestMatchesLog(ctx, t, records, drivers)
		e.checkGroupAssignment(ctx, t, partitions)
	})
	if t.Failed() {
		return
	}

	t.Run("restarted consumer picks up the backlog published while it was down", func(t *testing.T) {
		// The previous stage drained the group, so its committed offsets are at these
		// high watermarks.
		before := e.highWatermarks(ctx, t, partitions)
		e.dockerAction(ctx, t, "stop", "analytics-service")

		// Publish one round with the consumer down, then stop the producer so the
		// backlog is fixed.
		e.dockerAction(ctx, t, "start", "driver-kafka-producer")
		e.waitFor(ctx, t, "a new publish round from every driver", func() (bool, string) {
			seen := map[string]bool{}
			for _, r := range e.readLog(ctx, t, partitions) {
				if r.offset >= before[r.partition] {
					seen[r.event.DriverID] = true
				}
			}
			return len(seen) == len(drivers), fmt.Sprintf("new events from %d of %d drivers", len(seen), len(drivers))
		})
		e.dockerAction(ctx, t, "stop", "driver-kafka-producer")

		after := e.highWatermarks(ctx, t, partitions)
		var backlog int64
		for _, p := range partitions {
			backlog += after[p] - before[p]
		}
		committed := e.committedOffsets(ctx, t, partitions)
		for _, p := range partitions {
			if want := committedFor(before[p]); committed[p] != want {
				t.Fatalf("partition %d: committed offset %d while the consumer was down, want %d", p, committed[p], want)
			}
		}
		t.Logf("backlog of %d event(s) behind committed offsets %v", backlog, committed)

		// Resuming at the committed offsets reads the backlog and nothing else. A
		// consumer that restarted from the first offset would count the whole log and
		// fail waitForDrained at once; one that skipped to the end would count nothing
		// and time out.
		e.dockerAction(ctx, t, "start", "analytics-service")
		e.waitForReady(ctx, t, "analytics-service", e.analyticsURL)
		e.checkGroupAssignment(ctx, t, partitions)
		e.waitForDrained(ctx, t, partitions, backlog)
		e.checkLatestMatchesLog(ctx, t, e.readLog(ctx, t, partitions), drivers)
		e.checkMetrics(ctx, t, backlog)
	})
}

// checkKeyingAndOrder asserts the three properties the telemetry design relies on:
// the record key is the driver_id, all of one driver's events land on one partition,
// and that partition is the one kafka-go's Hash balancer picks; within the partition a
// driver's events are in publish order.
func checkKeyingAndOrder(t *testing.T, records []logRecord, drivers []string, partitionCount int) {
	t.Helper()
	known := map[string]bool{}
	for _, d := range drivers {
		known[d] = true
	}
	partitionOf := map[string]int{}
	lastTime := map[string]time.Time{}
	eventIDs := map[string]bool{}
	for _, r := range records {
		ev := r.event
		if !known[ev.DriverID] {
			t.Fatalf("partition %d offset %d: driver %q is not one of the producer's drivers %v", r.partition, r.offset, ev.DriverID, drivers)
		}
		if r.key != ev.DriverID {
			t.Fatalf("partition %d offset %d: key %q, want the driver_id %q", r.partition, r.offset, r.key, ev.DriverID)
		}
		if ev.EventType != events.TypeDriverLocationUpdated {
			t.Fatalf("partition %d offset %d: event_type %q, want %q", r.partition, r.offset, ev.EventType, events.TypeDriverLocationUpdated)
		}
		if ev.EventID == "" || eventIDs[ev.EventID] {
			t.Fatalf("partition %d offset %d: event_id %q is empty or repeated", r.partition, r.offset, ev.EventID)
		}
		eventIDs[ev.EventID] = true

		if want := expectedPartition(ev.DriverID, partitionCount); r.partition != want {
			t.Fatalf("driver %s written to partition %d, want %d (FNV-1a hash of the key)", ev.DriverID, r.partition, want)
		}
		if p, ok := partitionOf[ev.DriverID]; ok && p != r.partition {
			t.Fatalf("driver %s appears on partitions %d and %d", ev.DriverID, p, r.partition)
		}
		partitionOf[ev.DriverID] = r.partition

		at, err := time.Parse(time.RFC3339Nano, ev.Timestamp)
		if err != nil {
			t.Fatalf("partition %d offset %d: timestamp %q: %v", r.partition, r.offset, ev.Timestamp, err)
		}
		if prev, ok := lastTime[ev.DriverID]; ok && !at.After(prev) {
			t.Fatalf("driver %s: offset %d has timestamp %s, not after the previous event's %s", ev.DriverID, r.offset, at, prev)
		}
		lastTime[ev.DriverID] = at
	}
	t.Logf("%d events; driver -> partition %v", len(records), partitionOf)
}

// expectedPartition recomputes kafka-go's Hash balancer independently: FNV-1a over the
// key, the uint32 sum reinterpreted as int32 (Sarama-compatible), modulo the partition
// count, absolute value.
func expectedPartition(key string, partitionCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	p := int32(h.Sum32()) % int32(partitionCount)
	if p < 0 {
		p = -p
	}
	return int(p)
}

// waitForDrained waits until analytics-service's running total equals `want` and the
// group's committed offset has reached the high watermark on every partition.
//
// These are aggregate checks. A total above `want` proves that some event was processed
// more than once and fails at once, but an equal total does not prove that each event
// was processed once: a skipped event and a duplicated one cancel out. The service
// exposes no per-event record of what it handled, so no per-event claim is made.
func (e *env) waitForDrained(ctx context.Context, t *testing.T, partitions []int, want int64) {
	t.Helper()
	hw := e.highWatermarks(ctx, t, partitions)
	e.waitFor(ctx, t, fmt.Sprintf("analytics-service's total to reach %d and its commits the high watermarks %v", want, hw), func() (bool, string) {
		view := e.analytics(ctx, t)
		if view.TotalConsumed > want {
			t.Fatalf("analytics-service counted %d events but only %d were there for it to read: at least one was processed more than once", view.TotalConsumed, want)
		}
		committed := e.committedOffsets(ctx, t, partitions)
		for _, p := range partitions {
			if committed[p] != committedFor(hw[p]) {
				return false, fmt.Sprintf("consumed %d, committed %v", view.TotalConsumed, committed)
			}
		}
		return view.TotalConsumed == want, fmt.Sprintf("consumed %d, committed %v", view.TotalConsumed, committed)
	})
}

// committedFor is the committed offset a drained group reports for a partition whose
// high watermark is hw: hw itself, or -1 for a partition that has never held a record.
// With the four simulated drivers no key hashes to one of the three partitions, so an
// empty partition is the normal case.
func committedFor(hw int64) int64 {
	if hw == 0 {
		return -1
	}
	return hw
}

// checkLatestMatchesLog compares analytics-service's per-driver view with the last event
// for that driver in the log. It is only meaningful once the consumer has drained, and
// the view holds one location per driver, so earlier events are not checked one by one.
func (e *env) checkLatestMatchesLog(ctx context.Context, t *testing.T, records []logRecord, drivers []string) {
	t.Helper()
	last := map[string]sharedkafka.DriverLocationEvent{}
	for _, r := range records { // offsets ascend within a partition and a driver has one partition
		last[r.event.DriverID] = r.event
	}
	view := e.analytics(ctx, t)
	if view.Topic != sharedkafka.DriverLocationTopic || view.ConsumerGroup != consumerGroup {
		t.Fatalf("analytics-service reports topic %q group %q, want %q %q", view.Topic, view.ConsumerGroup, sharedkafka.DriverLocationTopic, consumerGroup)
	}
	if len(view.Drivers) != len(drivers) {
		t.Fatalf("analytics-service lists %d drivers, want %d: %+v", len(view.Drivers), len(drivers), view.Drivers)
	}
	for _, got := range view.Drivers {
		want, ok := last[got.DriverID]
		if !ok {
			t.Fatalf("analytics-service lists driver %s, which has no event in the log", got.DriverID)
		}
		wantTime, _ := time.Parse(time.RFC3339Nano, want.Timestamp)
		if got.Lat != want.Lat || got.Lng != want.Lng || got.Available != want.Available || !got.Timestamp.Equal(wantTime) {
			t.Fatalf("driver %s: analytics-service has %+v, last event in the log is %+v", got.DriverID, got, want)
		}
	}
}

// checkGroupAssignment waits for the consumer group to be Stable with one member that
// owns every partition of the topic.
func (e *env) checkGroupAssignment(ctx context.Context, t *testing.T, partitions []int) {
	t.Helper()
	e.waitFor(ctx, t, "consumer group "+consumerGroup+" to be Stable with one member owning every partition", func() (bool, string) {
		resp, err := e.client.DescribeGroups(ctx, &kafkago.DescribeGroupsRequest{GroupIDs: []string{consumerGroup}})
		if err != nil {
			return false, err.Error()
		}
		if len(resp.Groups) != 1 || resp.Groups[0].Error != nil {
			return false, fmt.Sprintf("%+v", resp.Groups)
		}
		g := resp.Groups[0]
		if g.GroupState != "Stable" || len(g.Members) != 1 {
			return false, fmt.Sprintf("state %s with %d member(s)", g.GroupState, len(g.Members))
		}
		var owned []int
		for _, topic := range g.Members[0].MemberAssignments.Topics {
			if topic.Topic == sharedkafka.DriverLocationTopic {
				owned = append(owned, topic.Partitions...)
			}
		}
		sort.Ints(owned)
		return fmt.Sprint(owned) == fmt.Sprint(partitions), fmt.Sprintf("member owns %v", owned)
	})
}

// checkMetrics compares the Prometheus counters with the HTTP view after a restart,
// when both started from zero.
func (e *env) checkMetrics(ctx context.Context, t *testing.T, want int64) {
	t.Helper()
	body := e.get(ctx, t, e.analyticsURL+"/metrics")
	values := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 {
			values[fields[0]] = fields[1]
		}
	}
	for name, expected := range map[string]int64{
		"metroride_kafka_driver_location_events_total": want,
		"metroride_kafka_consume_errors_total":         0,
	} {
		got, err := strconv.ParseFloat(values[name], 64)
		if err != nil || int64(got) != expected {
			t.Fatalf("metric %s = %q, want %d", name, values[name], expected)
		}
	}
}

// --- Kafka reads ------------------------------------------------------------------------

func (e *env) topicPartitions(ctx context.Context, t *testing.T) []int {
	t.Helper()
	resp, err := e.client.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{sharedkafka.DriverLocationTopic}})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if len(resp.Topics) != 1 || resp.Topics[0].Error != nil {
		t.Fatalf("metadata for %s: %+v", sharedkafka.DriverLocationTopic, resp.Topics)
	}
	var ids []int
	for _, p := range resp.Topics[0].Partitions {
		ids = append(ids, p.ID)
	}
	sort.Ints(ids)
	return ids
}

// highWatermarks returns the next offset to be written on each partition.
func (e *env) highWatermarks(ctx context.Context, t *testing.T, partitions []int) map[int]int64 {
	t.Helper()
	var reqs []kafkago.OffsetRequest
	for _, p := range partitions {
		reqs = append(reqs, kafkago.LastOffsetOf(p))
	}
	resp, err := e.client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{
		Topics: map[string][]kafkago.OffsetRequest{sharedkafka.DriverLocationTopic: reqs},
	})
	if err != nil {
		t.Fatalf("list offsets: %v", err)
	}
	out := map[int]int64{}
	for _, p := range resp.Topics[sharedkafka.DriverLocationTopic] {
		if p.Error != nil {
			t.Fatalf("list offsets partition %d: %v", p.Partition, p.Error)
		}
		out[p.Partition] = p.LastOffset
	}
	return out
}

func (e *env) committedOffsets(ctx context.Context, t *testing.T, partitions []int) map[int]int64 {
	t.Helper()
	resp, err := e.client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		GroupID: consumerGroup,
		Topics:  map[string][]int{sharedkafka.DriverLocationTopic: partitions},
	})
	if err != nil {
		t.Fatalf("offset fetch: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("offset fetch: %v", resp.Error)
	}
	out := map[int]int64{}
	for _, p := range resp.Topics[sharedkafka.DriverLocationTopic] {
		if p.Error != nil {
			t.Fatalf("offset fetch partition %d: %v", p.Partition, p.Error)
		}
		out[p.Partition] = p.CommittedOffset // -1 when nothing is committed yet
	}
	return out
}

// readLog reads every record currently in the topic, partition by partition, up to the
// high watermarks taken at the start of the call.
func (e *env) readLog(ctx context.Context, t *testing.T, partitions []int) []logRecord {
	t.Helper()
	hw := e.highWatermarks(ctx, t, partitions)
	var out []logRecord
	for _, p := range partitions {
		if hw[p] == 0 {
			continue
		}
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:   []string{e.broker},
			Topic:     sharedkafka.DriverLocationTopic,
			Partition: p,
			MinBytes:  1,
			MaxBytes:  1e6,
			MaxWait:   500 * time.Millisecond,
		})
		readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		for {
			msg, err := reader.ReadMessage(readCtx)
			if err != nil {
				cancel()
				_ = reader.Close()
				t.Fatalf("read partition %d below high watermark %d: %v", p, hw[p], err)
			}
			var ev sharedkafka.DriverLocationEvent
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				t.Fatalf("partition %d offset %d: decode %q: %v", p, msg.Offset, msg.Value, err)
			}
			out = append(out, logRecord{partition: p, offset: msg.Offset, key: string(msg.Key), event: ev})
			if msg.Offset >= hw[p]-1 {
				break
			}
		}
		cancel()
		_ = reader.Close()
	}
	return out
}

func (e *env) waitForLog(ctx context.Context, t *testing.T, partitions []int, done func([]logRecord) bool) []logRecord {
	t.Helper()
	var records []logRecord
	e.waitFor(ctx, t, "at least two events from every driver", func() (bool, string) {
		records = e.readLog(ctx, t, partitions)
		return done(records), fmt.Sprintf("%d events in the topic", len(records))
	})
	return records
}

// --- HTTP ----------------------------------------------------------------------------

func (e *env) analytics(ctx context.Context, t *testing.T) analyticsView {
	t.Helper()
	var view analyticsView
	if err := json.Unmarshal([]byte(e.get(ctx, t, e.analyticsURL+"/v1/analytics/drivers")), &view); err != nil {
		t.Fatalf("decode analytics view: %v", err)
	}
	return view
}

func (e *env) simulatedDrivers(ctx context.Context, t *testing.T) []string {
	t.Helper()
	var body struct {
		Drivers []struct {
			ID string `json:"id"`
		} `json:"drivers"`
	}
	if err := json.Unmarshal([]byte(e.get(ctx, t, e.producerURL+"/v1/drivers")), &body); err != nil {
		t.Fatalf("decode producer drivers: %v", err)
	}
	var ids []string
	for _, d := range body.Drivers {
		ids = append(ids, d.ID)
	}
	if len(ids) == 0 {
		t.Fatal("the telemetry producer reports no simulated drivers")
	}
	sort.Strings(ids)
	return ids
}

func (e *env) waitForReady(ctx context.Context, t *testing.T, service, baseURL string) {
	t.Helper()
	e.waitFor(ctx, t, service+" /readyz", func() (bool, string) {
		reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		defer cancel()
		req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, baseURL+"/readyz", nil)
		resp, err := e.http.Do(req)
		if err != nil {
			return false, err.Error()
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK, resp.Status
	})
}

func (e *env) get(ctx context.Context, t *testing.T, url string) string {
	t.Helper()
	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp, err := e.http.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %s, err %v, body %s", url, resp.Status, err, body)
	}
	return string(body)
}

// --- plumbing ------------------------------------------------------------------------

func newEnv(t *testing.T) *env {
	t.Helper()
	project := os.Getenv("KAFKA_E2E_PROJECT")
	if project == "" {
		t.Fatal("set KAFKA_E2E_PROJECT to a disposable Compose project; run through scripts/kafka-e2e-test.sh")
	}
	broker := getenv("INTEGRATION_KAFKA_BROKER", "kafka:9092")
	return &env{
		project:      project,
		broker:       broker,
		analyticsURL: getenv("INTEGRATION_ANALYTICS_URL", "http://analytics-service:8086"),
		producerURL:  getenv("INTEGRATION_PRODUCER_URL", "http://driver-kafka-producer:18081"),
		client:       &kafkago.Client{Addr: kafkago.TCP(broker), Timeout: 10 * time.Second},
		http:         &http.Client{Timeout: requestTimeout},
	}
}

// dockerAction stops, starts or restarts the one container of `service` in this run's
// Compose project. Containers are found by Compose's labels, so nothing outside the
// project can match.
func (e *env) dockerAction(ctx context.Context, t *testing.T, action, service string) {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "ps", "-aq",
		"--filter", "label=com.docker.compose.project="+e.project,
		"--filter", "label=com.docker.compose.service="+service).Output()
	if err != nil {
		t.Fatalf("find %s container: %v", service, err)
	}
	ids := strings.Fields(string(out))
	if len(ids) != 1 {
		t.Fatalf("project %s has %d %s container(s), want 1", e.project, len(ids), service)
	}
	if out, err := exec.CommandContext(ctx, "docker", action, ids[0]).CombinedOutput(); err != nil {
		t.Fatalf("docker %s %s: %v %s", action, service, err, out)
	}
	t.Logf("docker %s %s", action, service)
}

func (e *env) waitFor(ctx context.Context, t *testing.T, what string, check func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(stageTimeout)
	last := ""
	for {
		ok, state := check()
		if ok {
			return
		}
		last = state
		if time.Now().After(deadline) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("timed out after %s waiting for %s; last state: %s", stageTimeout, what, last)
		}
		time.Sleep(pollInterval)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
