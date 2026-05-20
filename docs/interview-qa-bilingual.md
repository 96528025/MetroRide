# MetroRide Bilingual Interview Q&A

This guide is for explaining MetroRide in Google/backend infrastructure interviews. MetroRide should be described honestly as a production-style local distributed systems project designed to demonstrate backend infrastructure concepts, not as a real production system operating at large scale.

## Q1. What happens after a rider requests a ride?

### 中文理解

简单理解：用户发出叫车请求后，系统先把订单保存下来，然后发一个事件通知调度服务去找司机。调度服务会问路线服务哪个司机最近，然后把司机分配给这个 ride，最后发出通知事件。

工程解释：`rider-service` 接收 `POST /v1/rides`，把 ride 以 `requested` 状态写入 PostgreSQL，然后向 Redis Streams 发布 `ride_requested`。`dispatch-service` 通过 consumer group 消费这个事件，检查 ride 是否已经被分配，调用 `routing-service` 找最近司机，再把 ride 更新为 `assigned` 并发布 assignment 和 notification 事件。

### English answer to memorize

When a rider requests a ride, the rider service first persists the ride in PostgreSQL with a requested status. It then publishes a `ride_requested` event to Redis Streams. The dispatch service consumes that event through a consumer group, checks whether the ride has already been assigned, and calls the routing service to find a nearby available driver. Once a driver is selected, dispatch updates the ride state in PostgreSQL and emits a `ride_assigned` event. A notification service then consumes the assignment event and simulates notifying the rider and driver. The key idea is that ride intake is fast and durable, while dispatch happens asynchronously.

### Keywords

- event-driven architecture
- Redis Streams
- PostgreSQL
- dispatch workflow
- consumer group
- asynchronous processing

## Q2. Why microservices?

### 中文理解

简单理解：每个服务负责一件清楚的事情，比如 rider 负责接收请求，dispatch 负责派单，routing 负责算路线。这样一个服务出问题时，不会把所有功能都绑死在一起。

工程解释：MetroRide 的不同部分有不同的扩展方式和失败模式。司机位置更新是高频事件，派单是异步工作流，路线计算是计算型服务，通知是副作用。拆成 microservices 可以展示清晰的 service boundary、独立部署、独立扩展和更好的故障隔离。

### English answer to memorize

I chose microservices because the ride dispatch domain naturally breaks into components with different responsibilities and scaling patterns. Rider intake is latency-sensitive, driver location updates are stream-heavy, dispatch is workflow-oriented, routing is compute-oriented, and notifications are side effects. By separating these into services, each component has a clear ownership boundary and can be scaled or evolved independently. It also makes failure isolation easier because notification or routing problems do not have to directly block the rider API. For a small real product, a modular monolith could be simpler, but MetroRide is intentionally designed to demonstrate distributed backend architecture.

### Keywords

- microservices
- service boundaries
- independent scaling
- fault isolation
- ownership
- modular architecture

## Q3. Why Redis Streams instead of direct REST calls?

### 中文理解

简单理解：如果所有服务都用 REST 直接互相调用，请求链会很长，一个服务慢了就会拖慢整个系统。Redis Streams 像一个事件队列，rider-service 只要把事件发出去，dispatch-service 可以异步处理。

工程解释：Redis Streams 提供 durable stream、consumer group 和 ack 机制。它让 ride intake 和 dispatch 解耦，支持重试、回放和 backpressure。REST 仍然用于 dispatch 调 routing，因为派单需要同步拿到路线结果。

### English answer to memorize

Direct REST calls would tightly couple the rider request path to every downstream operation. Redis Streams lets the rider service persist the ride and publish an event, while dispatch processes that event asynchronously. This gives the system better decoupling, backpressure tolerance, and replay behavior. Redis Streams also supports consumer groups, so dispatch workers can scale horizontally. I still use REST where a synchronous answer is required, such as dispatch asking routing for a driver decision. So the design uses asynchronous events for workflow coordination and synchronous calls only where they are on the critical decision path.

### Keywords

- Redis Streams
- asynchronous workflow
- consumer groups
- backpressure
- replay
- REST tradeoff

## Q4. Why is dispatch-service separated?

### 中文理解

简单理解：dispatch-service 是派单大脑，负责把 ride request 变成 driver assignment。它不应该和 rider API 混在一起，否则 rider 请求会被复杂的派单逻辑拖慢。

工程解释：dispatch-service 负责消费事件、重试、幂等检查、调用 routing、写 assignment、发 dead-letter。把它独立出来，可以单独扩展 worker 数量，也可以把派单失败和 rider-service 隔离。

### English answer to memorize

I separated dispatch because it owns the assignment workflow, which is different from accepting rider API requests. Dispatch consumes ride events, applies idempotency checks, calls routing, persists assignments, emits downstream events, and handles dead-letter behavior. That workflow can fail or slow down independently from ride intake, so separating it protects the rider-facing API. It also allows dispatch workers to scale horizontally through Redis consumer groups. In a production system, dispatch would likely be one of the most operationally important services because it coordinates the core business workflow.

### Keywords

- dispatch workflow
- assignment orchestration
- consumer group
- idempotency
- horizontal scaling
- failure isolation

## Q5. Why is routing-service separated?

### 中文理解

简单理解：routing-service 专门负责找最近司机和计算 ETA。这个逻辑可能以后会变复杂，比如加入交通、地图、机器学习 ETA，所以应该独立出来。

工程解释：routing 是计算型服务，依赖 driver location state。把 routing 单独拆出来，可以让 dispatch 保持工作流职责，让 routing 独立优化算法、缓存、地理分区和性能。

### English answer to memorize

Routing is separated because it has a different responsibility and evolution path from dispatch. Dispatch coordinates the workflow, while routing answers the question of which driver is closest and what the estimated arrival time is. In the current project, routing uses simulated driver locations, but in a larger system it could evolve into a much more complex service with traffic data, geospatial indexing, caching, and ML-based ETA prediction. Keeping it separate lets the routing algorithm improve without changing the dispatch workflow. It also makes routing easier to scale independently if route computation becomes expensive.

### Keywords

- routing service
- ETA
- geospatial state
- compute service
- separation of concerns
- independent optimization

## Q6. Why PostgreSQL?

### 中文理解

简单理解：PostgreSQL 保存真实的 ride 状态，比如 requested 或 assigned。Redis 负责传事件，但不能代替数据库保存长期状态。

工程解释：PostgreSQL 是 system of record，适合事务、索引、约束和查询。Redis Streams 只负责 workflow coordination。这样可以明确区分 durable state 和 event transport。

### English answer to memorize

PostgreSQL is used as the durable system of record for ride and assignment state. Redis Streams coordinates workflow events, but it should not be treated as the long-term database for ride status. PostgreSQL gives the system transactional writes, indexes, constraints, and straightforward status queries. For example, dispatch uses PostgreSQL state to prevent duplicate assignments. This separation is important: the event bus moves work through the system, while PostgreSQL owns the authoritative state.

### Keywords

- PostgreSQL
- system of record
- durable state
- transactions
- authoritative status
- state vs event transport

## Q7. Why Prometheus and Grafana?

### 中文理解

简单理解：Prometheus 收集 metrics，Grafana 展示 dashboard。这样可以看到系统有没有正常运行，比如请求量、派单延迟、失败次数和司机数量。

工程解释：每个服务暴露 `/metrics`，Prometheus scrape，Grafana 可视化。这个组合是常见的 cloud-native observability stack，适合展示 request rate、latency、error、dependency failure 和 active driver gauge。

### English answer to memorize

Prometheus and Grafana give MetroRide an infrastructure-style observability story. Each service exposes Prometheus metrics, and Prometheus scrapes metrics such as ride request count, assignment count, dispatch latency, routing duration, dependency errors, stream consume errors, and active drivers. Grafana turns those metrics into dashboards so the system can be inspected while it runs locally. This is important because distributed systems are hard to understand from logs alone. Metrics help answer whether the system is healthy, whether dispatch is falling behind, and whether dependencies are failing.

### Keywords

- Prometheus
- Grafana
- observability
- metrics
- dashboards
- latency
- dependency errors

## Q8. What is idempotency and why does it matter?

### 中文理解

简单理解：同一个事件处理多次，结果应该和处理一次一样。比如同一个 ride request 被重复消费，不能分配两个司机。

工程解释：Redis Streams 可能 redeliver message，服务也可能在 ack 前 crash。dispatch-service 会先查 PostgreSQL，如果 ride 已经 assigned 就跳过，并且 update SQL 使用 `where status = 'requested'` 防止并发重复分配。

### English answer to memorize

Idempotency means that processing the same operation multiple times has the same effect as processing it once. In MetroRide, this matters because Redis Streams can redeliver events after crashes or restarts. If the same `ride_requested` event is processed twice, the system must not assign two drivers to the same ride. Dispatch handles this by checking PostgreSQL before assignment and only updating rides that are still in `requested` status. This makes duplicate delivery safe and gives the system more reliable failure recovery behavior.

### Keywords

- idempotency
- duplicate events
- Redis redelivery
- `status = requested`
- concurrency safety
- failure recovery

## Q9. What is a dead-letter stream and why is it important?

### 中文理解

简单理解：如果一个事件重试多次还是处理失败，就不要一直卡住主流程，而是放到一个失败事件流里，之后人工或工具检查。

工程解释：MetroRide 使用 `events.dead_letter` 保存 dispatch 处理失败的事件，包括原事件类型、ride_id、错误信息、服务名和时间。这样 poison message 不会无限阻塞 consumer group，也方便排查。

### English answer to memorize

A dead-letter stream is a place to put events that cannot be processed successfully after retries. In MetroRide, if dispatch cannot process a `ride_requested` event after bounded retries, it publishes a failure record to `events.dead_letter`. That record includes the original event type, ride ID, error message, service name, and timestamp. This is important because a poison message should not block the consumer group forever. It also gives operators a clear place to inspect failed work and decide whether to replay or repair it.

### Keywords

- dead-letter stream
- failed events
- bounded retries
- poison message
- operational recovery
- `events.dead_letter`

## Q10. What happens if dispatch-service crashes?

### 中文理解

简单理解：如果 dispatch-service 在处理事件时 crash，Redis Streams 会保留还没 ack 的消息。服务恢复后可以继续处理。即使重复处理，idempotency 也会防止重复派单。

工程解释：Redis consumer group 有 pending message 机制。dispatch 在成功处理后才 ack。crash 前未 ack 的事件不会消失，恢复后可被重新处理。PostgreSQL 状态检查保护重复 assignment。

### English answer to memorize

If dispatch-service crashes before acknowledging a Redis Stream message, the message remains in the consumer group's pending state. That means the work is not lost. When dispatch comes back, a consumer can recover or reprocess the message. If the ride had already been assigned before the crash, the idempotency check prevents assigning another driver. This is why the system combines stream acknowledgement semantics with PostgreSQL-backed assignment state.

### Keywords

- dispatch-service
- crash recovery
- pending messages
- acknowledgements
- consumer group
- idempotency

## Q11. What happens if Redis goes down?

### 中文理解

简单理解：Redis down 时，依赖 Redis 的服务 readiness 会失败，事件发布和消费会失败。ride 可能已经写入 PostgreSQL，但事件没发出去，这就是未来要用 outbox pattern 改进的地方。

工程解释：当前系统会记录 dependency error metrics 和 structured logs。Redis 是事件总线，down 了会影响异步工作流。更生产化的设计会使用 transactional outbox，保证 DB 写入后事件最终可恢复发布。

### English answer to memorize

If Redis goes down, Redis-dependent readiness checks fail and services cannot publish or consume workflow events. The system records dependency error metrics and structured logs so the failure is visible. A ride request may still be persisted in PostgreSQL, but event publication can fail, which means dispatch may not receive the request immediately. This is an honest limitation of the current local design. In a more production-ready version, I would add a transactional outbox so database writes and eventual event publication are recoverable.

### Keywords

- Redis outage
- readiness checks
- dependency errors
- event publication failure
- transactional outbox
- eventual recovery

## Q12. What happens if routing-service times out?

### 中文理解

简单理解：dispatch-service 调 routing-service 找司机。如果 routing timeout，dispatch 会重试几次。如果还是失败，就把事件放到 dead-letter stream，ride 保持 requested 状态。

工程解释：routing 在派单 critical path 上，所以必须有 timeout，避免 worker 无限等待。bounded retry 处理短暂故障，dead-letter 处理持续失败。

### English answer to memorize

Routing is on the critical path for assignment, so dispatch calls it with an explicit timeout. If the routing request times out, dispatch retries a bounded number of times. If all retries fail, the ride is not assigned, and the failed event is published to the dead-letter stream. The ride remains in a requested state in PostgreSQL. This prevents the dispatch worker from hanging indefinitely while still preserving the failed work for inspection or replay.

### Keywords

- routing timeout
- bounded retries
- critical path
- dead-letter stream
- requested state
- failure handling

## Q13. How do you handle duplicate events?

### 中文理解

简单理解：重复事件是正常的分布式系统问题。处理方式不是假设它不会发生，而是让处理逻辑安全。

工程解释：dispatch-service 通过 PostgreSQL 状态做 dedupe。如果 ride 已经 assigned 或不再是 requested，就 skip。SQL update 也带状态条件，避免并发 worker 重复写入。

### English answer to memorize

I handle duplicate events through idempotent dispatch logic. Dispatch does not assume that every stream event is delivered exactly once. Before assigning a ride, it checks PostgreSQL to see whether the ride is still in the requested state and has no driver. The assignment update is also guarded by `status = 'requested'`. If a duplicate event arrives later, dispatch simply skips it because the ride has already been assigned.

### Keywords

- duplicate events
- idempotent consumer
- PostgreSQL state check
- conditional update
- at-least-once delivery
- safe replay

## Q14. How would the system scale to 1 million rides per day?

### 中文理解

简单理解：如果流量很大，就需要按区域拆分、增加 dispatch workers、用 Kafka、优化数据库、增加 tracing 和 autoscaling。

工程解释：1 million rides/day 需要更强的 event backbone、partition strategy、regional sharding、database indexing/partitioning、routing cache/location index、Kubernetes autoscaling 和 observability。Redis Streams 可以迁移到 Kafka。

### English answer to memorize

To scale toward 1 million rides per day, I would first partition the system by region or geohash so ride requests and driver locations are processed close to their area. I would likely migrate from Redis Streams to Kafka for stronger partitioning, retention, and throughput. Dispatch workers could autoscale based on stream lag and assignment latency. PostgreSQL would need indexing, partitioning, connection pool tuning, and potentially read replicas. Routing would need a better geospatial index or location store, and I would add distributed tracing to understand latency across services. The current project is local and production-style, but these are the changes I would make for real scale.

### Keywords

- Kafka
- partitioning
- geohash
- autoscaling
- stream lag
- PostgreSQL partitioning
- distributed tracing

## Q15. What are the main bottlenecks?

### 中文理解

简单理解：主要瓶颈是 Redis Streams 的吞吐、routing 同步调用、PostgreSQL 写入、以及 routing-service 里的内存司机状态。

工程解释：dispatch 依赖 routing 的同步响应，PostgreSQL 是 durable write path，Redis 是 event backbone。高规模下需要 partition、Kafka、outbox、routing state sharding 和 DB tuning。

### English answer to memorize

The main bottlenecks are the event backbone, the synchronous routing call, PostgreSQL write throughput, and routing's in-memory driver state. Redis Streams is good for the local project, but Kafka would be better for higher throughput and partitioned fanout. Dispatch also depends on routing, so routing latency directly affects assignment latency. PostgreSQL is the durable write path, so indexes, pooling, and partitioning would matter at scale. Routing state would also need to be partitioned by region instead of kept as a simple in-memory view.

### Keywords

- bottlenecks
- Redis Streams throughput
- routing latency
- PostgreSQL writes
- in-memory state
- partitioning

## Q16. Why not Kafka from the beginning?

### 中文理解

简单理解：Kafka 更强大，但本地项目一开始用 Kafka 会增加很多运维复杂度。Redis Streams 足够展示 event-driven、consumer group、ack 和 retry。

工程解释：技术选择要和项目阶段匹配。MetroRide 是 local production-style project，不是真实大规模系统。Redis Streams 保持可运行性，同时事件 envelope 保留未来迁移 Kafka 的空间。

### English answer to memorize

Kafka would be a strong choice for a real high-volume event platform, but it adds operational complexity for a local portfolio project. Redis Streams gives me the core concepts I wanted to demonstrate: durable streams, consumer groups, acknowledgements, retries, and replay behavior. It keeps the system easy to run with Docker Compose. I also designed the event envelope so the transport can be replaced later. So Redis Streams is a pragmatic starting point, while Kafka is a clear future scaling path.

### Keywords

- Kafka tradeoff
- Redis Streams
- operational complexity
- local development
- migration path
- event envelope

## Q17. Why not Kubernetes from the beginning?

### 中文理解

简单理解：本地开发用 Docker Compose 更简单，更容易让项目跑起来。Kubernetes 适合部署阶段，所以 repo 里有 k8s manifests 和 Helm scaffold。

工程解释：Compose 降低本地运行成本；Kubernetes artifacts 展示 cloud-native direction。这样既能快速开发，又能说明未来如何迁移到真实 orchestration。

### English answer to memorize

Docker Compose is the right starting point because it makes the full local system easy to run and debug. Kubernetes is more realistic for production deployment, but it adds setup and operational overhead during early development. MetroRide includes Kubernetes manifests and Helm scaffolding to show the deployment direction without making local development harder. If I were deploying this in the cloud, I would use Kubernetes Deployments, Services, ConfigMaps, Secrets, managed Redis/PostgreSQL, resource limits, and autoscaling. So Compose is for local productivity, and Kubernetes is the production path.

### Keywords

- Docker Compose
- Kubernetes
- local orchestration
- cloud-native deployment
- Helm
- developer productivity

## Q18. What tradeoffs did you make?

### 中文理解

简单理解：这个项目为了清晰展示架构，选择了简单但真实的组件。比如用 Redis Streams 而不是 Kafka，用 REST 而不是 gRPC，用内存 routing state 而不是真实地图系统。

工程解释：tradeoff 是降低运维复杂度，换取本地可运行和清晰架构。未来可以加 Kafka、gRPC、outbox、OpenTelemetry、autoscaling 和 region partition。

### English answer to memorize

The main tradeoff was choosing simplicity where it helps the project remain runnable locally, while still preserving production-style patterns. I used Redis Streams instead of Kafka to reduce operational complexity. I used REST for internal routing calls instead of gRPC because it is easier to inspect and sufficient for the MVP. Routing uses simulated in-memory driver state instead of a real geospatial system. I also do not claim exactly-once distributed transactions; a transactional outbox would be a future improvement. These tradeoffs keep the project understandable while still demonstrating distributed systems concepts honestly.

### Keywords

- tradeoffs
- simplicity
- local runnable system
- REST vs gRPC
- Redis vs Kafka
- transactional outbox

## Q19. How is this different from a CRUD app?

### 中文理解

简单理解：CRUD app 主要是 API 直接读写数据库。MetroRide 有多个服务、事件流、异步派单、重试、幂等、dead-letter、metrics 和 readiness checks。

工程解释：核心差异是 distributed workflow。ride request 不是简单 insert 后结束，而是触发 event-driven dispatch pipeline，并且有 observability 和 failure handling。

### English answer to memorize

MetroRide is different from a CRUD app because the main behavior is a distributed workflow, not just direct database operations. A ride request is persisted, published as an event, consumed by dispatch, routed to a driver, assigned idempotently, and emitted as downstream events. The system includes service boundaries, asynchronous messaging, retries, timeouts, dead-letter handling, health checks, readiness checks, metrics, and structured logs. Those are backend infrastructure concerns rather than simple CRUD concerns. The project is meant to show how services coordinate reliably under failure.

### Keywords

- distributed workflow
- event-driven pipeline
- asynchronous dispatch
- reliability
- observability
- service coordination

## Q20. What technical part are you most proud of?

### 中文理解

简单理解：最值得讲的是 dispatch-service 的可靠性设计，因为它把事件消费、幂等、防重复派单、重试、dead-letter 和 metrics 串起来了。

工程解释：dispatch 是核心 workflow coordinator。它展示了生产系统里很重要的能力：at-least-once event handling、idempotent state transition、bounded retry、failure isolation 和 observability。

### English answer to memorize

The part I am most proud of is the dispatch reliability design. Dispatch is the core workflow coordinator, so I made it handle at-least-once event delivery safely. It checks PostgreSQL state before assigning a ride, uses a conditional update to avoid duplicate assignment, applies bounded retries for transient failures, and publishes failed events to a dead-letter stream. It also emits metrics for assignment latency, failures, dependency errors, and stream consume errors. That part best represents the production engineering mindset behind the project.

### Keywords

- dispatch-service
- reliability
- idempotency
- at-least-once delivery
- dead-letter stream
- production engineering

## 30-second project pitch

### 中文理解

MetroRide 是一个 production-style 的本地分布式后端项目，用 ride dispatch 场景展示 backend infrastructure 能力。它不是前端项目，也不是简单 CRUD。重点是 Go microservices、Redis Streams 事件驱动、PostgreSQL 持久化状态、dispatch 派单流程、Prometheus/Grafana 可观测性，以及可靠性设计。

### English version to memorize

MetroRide is a production-style local distributed ride dispatch platform built to demonstrate backend infrastructure concepts. It uses Go microservices, Redis Streams for event-driven coordination, PostgreSQL for durable ride state, and Prometheus/Grafana for observability. A ride request is persisted, published as an event, consumed by dispatch, routed to an available driver, assigned idempotently, and emitted as a notification event. The focus is service boundaries, asynchronous workflows, reliability, and cloud-native deployment patterns, not frontend features.

## 2-minute technical walkthrough

### 中文理解

先讲 rider-service 接收请求并写 PostgreSQL，再讲 Redis Streams 发布 `ride_requested`。dispatch-service 通过 consumer group 消费事件，做幂等检查，然后调用 routing-service 找司机。routing-service 的司机状态来自 driver-service 的 location update。dispatch 成功后写 assignment，发布 `ride_assigned` 和 notification event。最后讲 reliability：timeout、retry、dead-letter、readiness、metrics。

### English version to memorize

MetroRide models the core backend workflow of ride dispatch. `rider-service` accepts a ride request over REST, writes the ride to PostgreSQL with a requested status, and publishes a `ride_requested` event to Redis Streams. `dispatch-service` consumes that event through a consumer group, checks PostgreSQL to avoid duplicate assignment, and calls `routing-service` to find a nearby available driver. `routing-service` maintains its driver view from location updates published by `driver-service`. Once dispatch selects a driver, it updates the ride to assigned, writes an assignment record, and emits assignment and notification events. The reliability layer includes explicit timeouts, bounded retries, dependency-aware readiness checks, idempotent dispatch logic, and a dead-letter stream for failed events. Observability comes from Prometheus metrics, Grafana dashboards, and structured JSON logs.

## 5-minute system design walkthrough

### 中文理解

从问题开始：ride dispatch 是实时协调问题，不应该让 rider API 同步等待所有下游步骤。然后讲服务拆分：rider 负责接请求，driver 负责位置，dispatch 负责工作流，routing 负责路线，notification 负责通知。再讲数据和事件：PostgreSQL 是真实状态，Redis Streams 是异步事件流。然后讲可靠性：dispatch crash 时 message 不会丢，重复事件靠 idempotency，routing timeout 进入 retry 和 dead-letter。最后讲扩展：Kafka、gRPC、OpenTelemetry、autoscaling、region partition 和 outbox。

### English version to memorize

I would start with the problem: ride dispatch is a real-time coordination workflow, and the rider API should not synchronously wait for every downstream side effect. MetroRide separates the domain into services with clear responsibilities. `rider-service` owns ride intake and durable state creation, `driver-service` publishes simulated location updates, `dispatch-service` coordinates assignment, `routing-service` computes the nearest driver, and `notification-service` handles downstream notification events. PostgreSQL is the system of record for ride and assignment state, while Redis Streams coordinates asynchronous workflow events. Dispatch consumes `ride_requested`, checks idempotency in PostgreSQL, calls routing, persists the assignment, and emits downstream events. Reliability comes from health and readiness endpoints, explicit timeouts, bounded retries, idempotent assignment, and a dead-letter stream for failed dispatch events. Observability comes from Prometheus metrics, Grafana dashboards, and structured logs. To scale this further, I would migrate eventing to Kafka, partition rides and drivers by region, add OpenTelemetry tracing, implement a transactional outbox, and autoscale dispatch workers based on stream lag and latency.
