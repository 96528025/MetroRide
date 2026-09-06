package com.metroride.fare.web;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Serves the Prometheus text exposition at {@code GET /metrics}, the path every Go service uses
 * and the one {@code infrastructure/prometheus/prometheus.yml} scrapes. The actuator's own
 * {@code /actuator/prometheus} stays available but is not part of the cross-service contract.
 */
@RestController
public class MetricsController {

    static final String PROMETHEUS_TEXT = "text/plain;version=0.0.4;charset=utf-8";

    private final PrometheusMeterRegistry registry;

    public MetricsController(PrometheusMeterRegistry registry) {
        this.registry = registry;
    }

    @GetMapping(value = "/metrics", produces = PROMETHEUS_TEXT)
    public String metrics() {
        return registry.scrape();
    }
}
