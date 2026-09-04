# MetroRide Performance Notes

MetroRide keeps performance claims reproducible and scoped. It does not claim production throughput from a local simulation.

## Nearest-Driver Selection

Routing needs one minimum-distance driver, not a globally sorted candidate list. `routing-service` therefore scans the in-memory driver map once:

- Time complexity: `O(n)` for `n` drivers.
- Additional space: `O(1)`.
- Distance calculations: one per available driver.
- Tie behavior: deterministic by driver ID.

The previous implementation copied all available drivers and sorted them, requiring `O(n)` additional space and `O(n log n)` comparisons while recalculating distances during sorting.

Run the committed benchmark with:

```bash
go test -run '^$' -bench BenchmarkSelectNearestDriver10000 -benchmem ./services/routing-service/cmd
```

One reference run on Darwin/arm64 with Go 1.22.12 selected from 10,000 simulated drivers in approximately `0.73 ms/op` with `0 B/op` and `0 allocs/op`. This is a developer-machine baseline, not a service-level latency or capacity claim. CI compiles and runs the correctness tests; future load testing should measure end-to-end request and dispatch latency under controlled concurrency.

## Next Measurements

- End-to-end ride request throughput and dispatch p50/p95/p99 latency.
- Outbox backlog drain rate after a dependency outage.
- Redis consumer lag as dispatch worker count changes.
- CPU and memory profiles for routing with region-sized driver sets.
