# Kafka Interview Notes

These notes explain the optional MetroRide Kafka extension in bilingual interview format. MetroRide remains a production-style local distributed systems project, not a real production deployment at scale.

## Why Kafka?

### 中文解释

Kafka 适合处理持续、大量、可回放的事件流。司机位置更新就是典型例子，因为同一个司机会不断产生 location event，而且这些事件可能被 analytics、ETA、monitoring 等多个系统消费。

### English interview answer

Kafka was added to demonstrate high-throughput streaming architecture concepts. Driver location updates are naturally stream-oriented: they are frequent, append-only, and useful to multiple downstream consumers. Kafka provides topics, partitions, consumer groups, offsets, and replay semantics. In MetroRide, Kafka is optional and focused only on driver telemetry, while Redis Streams continues to handle the core dispatch workflow.

### Keywords

- Kafka
- streaming telemetry
- topics
- partitions
- replay
- consumer groups

## Why not fully replace Redis Streams?

### 中文解释

Redis Streams 已经很适合 MetroRide 的本地 dispatch workflow。完全换成 Kafka 会增加运行成本和复杂度，而且这个项目的目标不是过度工程化。

### English interview answer

I did not fully replace Redis Streams because the existing dispatch workflow is lightweight and works well with Redis consumer groups and acknowledgements. Kafka is more powerful for high-volume streaming, but it also introduces more operational complexity. The goal was to show a realistic architecture evolution, not to migrate everything just because Kafka exists. MetroRide uses Redis Streams for workflow coordination and Kafka for optional driver telemetry streaming.

### Keywords

- Redis Streams
- Kafka tradeoff
- operational complexity
- workflow coordination
- architecture evolution

## Why are driver location updates a strong Kafka use case?

### 中文解释

司机位置更新频率高、数据连续、按时间追加，而且多个系统都可能需要消费。这种 telemetry stream 非常适合 Kafka。

### English interview answer

Driver location updates are a strong Kafka use case because they are continuous, append-only events. Multiple systems may want to consume them independently, such as analytics, ETA prediction, driver heatmaps, and monitoring. Kafka lets each consumer group process the same topic at its own pace. It also supports replay, which is useful when rebuilding analytics state or adding a new downstream consumer.

### Keywords

- driver telemetry
- append-only events
- fanout
- analytics
- ETA prediction
- replay

## What is a Kafka consumer group?

### 中文解释

Consumer group 是一组共同消费同一个 topic 的消费者。Kafka 会把 partitions 分配给 group 里的不同 consumer，让同一个 group 内每条消息只被一个 consumer 处理。

### English interview answer

A Kafka consumer group is a set of consumers that coordinate to process a topic. Kafka assigns partitions across consumers in the same group, so each message is processed by one consumer in that group. This is useful for horizontal scaling because adding more consumers can increase parallelism up to the number of partitions. MetroRide's analytics service uses the consumer group `metroride-analytics-service`.

### Keywords

- consumer group
- partition assignment
- horizontal scaling
- offsets
- parallelism

## Why partition by driver_id?

### 中文解释

用 `driver_id` 做 partition key 可以保证同一个司机的事件进入同一个 partition。Kafka 保证 partition 内有序，所以同一个司机的位置更新顺序不会乱。

### English interview answer

MetroRide partitions driver location events by `driver_id` so all events for the same driver go to the same Kafka partition. Kafka preserves ordering within a partition. That means downstream consumers process a driver's location updates in order, which matters because an older location should not overwrite a newer one. This is a realistic partitioning strategy for per-entity telemetry streams.

### Keywords

- partition key
- `driver_id`
- ordering
- per-driver stream
- telemetry consistency

## What is replay?

### 中文解释

Replay 指 consumer 可以从较早的 offset 重新读取历史事件。这样可以重建状态、debug，或者让新的 consumer 处理过去的数据。

### English interview answer

Replay means reading events from an earlier offset in a Kafka topic. Because Kafka stores events for a retention window, consumers can rebuild state, debug historical behavior, or bootstrap a new analytics service from past events. In MetroRide, replay is a concept demonstrated by using Kafka offsets and consumer groups, although local retention is intentionally short to keep disk usage low.

### Keywords

- replay
- offsets
- retention
- rebuild state
- historical events

## When would Kafka be better than Redis Streams?

### 中文解释

当事件量很大、消费者很多、需要强 partition、长期 retention、跨系统数据流时，Kafka 通常比 Redis Streams 更适合。

### English interview answer

Kafka is a better fit when the system needs high-throughput event streaming, strong partitioning, long retention, replay, and many independent consumers. Redis Streams is simpler and lighter for local workflow coordination. In a larger MetroRide system, Kafka would be a strong fit for driver telemetry, ETA pipelines, demand forecasting, and analytics fanout.

### Keywords

- high throughput
- long retention
- fanout
- partitioning
- analytics pipelines
- Kafka vs Redis Streams

## What operational complexity does Kafka introduce?

### 中文解释

Kafka 需要管理 brokers、topics、partitions、consumer lag、retention、磁盘、内存和 rebalance。它很强，但运维成本比 Redis Streams 高。

### English interview answer

Kafka introduces operational complexity around broker configuration, topic management, partition counts, consumer lag, retention, disk usage, memory, and rebalancing. It is powerful, but it is not free operationally. That is why MetroRide keeps Kafka optional and lightweight. The project demonstrates when Kafka is appropriate without forcing it into every part of the system.

### Keywords

- operational complexity
- brokers
- partitions
- consumer lag
- retention
- rebalancing
