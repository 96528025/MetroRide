# Lightweight Kafka Streaming Extension

MetroRide includes an optional Kafka extension to demonstrate streaming architecture concepts without replacing the existing Redis Streams dispatch workflow. Kafka is intentionally disabled by default and only starts with the `kafka` Docker Compose profile.

```bash
docker compose --profile kafka up -d
```

Normal development remains unchanged:

```bash
docker compose up -d
```

## Why Kafka Was Added

Kafka was added to demonstrate high-throughput streaming concepts that commonly appear in ride-sharing and logistics systems:

- Topics.
- Producers.
- Consumers.
- Consumer groups.
- Partition keys.
- Replay.
- Streaming analytics.

The Kafka extension focuses on driver location telemetry, a realistic stream-like workload where events are continuously produced and consumed independently from the dispatch workflow.

## Why Redis Streams Was Kept

MetroRide still uses Redis Streams for the core ride dispatch workflow. That path is intentionally unchanged:

1. `rider-service` publishes `ride_requested`.
2. `dispatch-service` consumes the request.
3. `dispatch-service` calls `routing-service`.
4. `dispatch-service` persists assignment state.
5. Assignment and notification events are emitted.

Redis Streams remains a good fit for the local MVP because it is lightweight, simple to operate, and already supports consumer groups and acknowledgement semantics.

## Redis Streams vs Kafka Tradeoffs

| Area | Redis Streams | Kafka |
| --- | --- | --- |
| Local setup | Lightweight and simple | Heavier, more operational concepts |
| Consumer groups | Supported | Supported |
| Replay | Possible with stream IDs and retention | Core architectural feature |
| Partitioning | More limited | First-class topic partitioning |
| High throughput | Good for moderate workloads | Better fit for high-volume event streams |
| Ecosystem | Smaller streaming ecosystem | Broad streaming and analytics ecosystem |
| Operational cost | Lower | Higher |

The project uses both intentionally: Redis Streams for lightweight workflow coordination and Kafka for optional streaming telemetry.

## Driver Location Events as a Kafka Use Case

Driver location updates are a strong Kafka use case because they are:

- Frequent.
- Append-only.
- Time ordered per driver.
- Useful to multiple downstream consumers.
- Valuable for replay, analytics, monitoring, and future ETA modeling.

MetroRide publishes low-frequency simulated driver location events to keep local CPU, memory, and disk usage low. In the Kafka Compose profile, a dedicated driver telemetry producer publishes Kafka events while Redis location publishing remains owned by the normal `driver-service` workflow.

## Topic

MetroRide creates one Kafka topic:

```text
metroride.driver.location.v1
```

The `.v1` suffix leaves room for future schema evolution.

## Partitioning Strategy

Kafka messages use:

```text
driver_id
```

as the partition key.

Partitioning by `driver_id` ensures that events for the same driver are sent to the same partition. Kafka preserves order within a partition, so a consumer sees each driver's location stream in order. This is important for telemetry because downstream systems should not process a driver's older location after a newer one.

## Consumer Groups

`analytics-service` consumes the driver location topic with:

```text
metroride-analytics-service
```

as its consumer group. A consumer group lets Kafka divide partitions across service instances. If the analytics service scaled to multiple replicas, Kafka would assign topic partitions across those replicas so each event is processed by one consumer in the group.

## Replay Concept

Kafka stores events in topic partitions for a retention window. Consumers track offsets. If a consumer group starts from an earlier offset, it can replay historical events. This makes Kafka useful for rebuilding analytics state, debugging, and introducing new consumers.

MetroRide keeps retention intentionally low in local Compose to avoid unnecessary disk usage.

## Lightweight Implementation

This extension is intentionally small:

- Single-node Kafka in KRaft mode.
- No ZooKeeper.
- One topic.
- Low heap settings.
- Short log retention.
- No persistent Kafka volume.
- One lightweight analytics consumer.
- Low-frequency driver location publishing.

The goal is to demonstrate architecture concepts without turning MetroRide into a heavy infrastructure lab.

## How This Could Evolve

At larger scale, MetroRide could evolve Kafka usage by:

- Moving high-volume event streams to Kafka.
- Adding schema registry and versioned event contracts.
- Partitioning topics by region and driver ID.
- Adding more consumers for ETA training, fraud detection, heatmaps, and supply analytics.
- Using managed Kafka in cloud environments.
- Adding OpenTelemetry tracing and consumer lag dashboards.
- Keeping Redis or another lightweight store for low-latency coordination where appropriate.

This repository does not claim production Kafka scale or benchmark throughput. It demonstrates a realistic architecture evolution path from lightweight event coordination toward higher-throughput streaming.
