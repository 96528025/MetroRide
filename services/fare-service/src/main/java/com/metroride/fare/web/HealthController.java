package com.metroride.fare.web;

import com.metroride.fare.consumer.ConsumerHalt;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.data.redis.RedisHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.actuate.jdbc.DataSourceHealthIndicator;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The same liveness and readiness contract as {@code httpx.CommonMuxWithReadiness} in
 * {@code shared/pkg/httpx/httpx.go}:
 *
 * <pre>
 *   GET /healthz  200 {"status":"ok"}
 *   GET /readyz   200 {"status":"ready"}
 *                 503 {"status":"not_ready","failures":{"postgres":"...","redis":"...","consumer":"..."}}
 * </pre>
 *
 * Readiness really talks to both dependencies; the checks are the actuator indicators, wired
 * explicitly so the names match the Go services' {@code postgres} and {@code redis} keys. The
 * third key, {@code consumer}, has no Go counterpart: it fails once the stream consumer has halted
 * on a fatal failure (see {@link ConsumerHalt}), with the reason as the message.
 */
@RestController
public class HealthController {

    private static final Logger log = LoggerFactory.getLogger(HealthController.class);

    private final Map<String, HealthIndicator> readinessChecks;

    @Autowired
    public HealthController(
            DataSource dataSource, RedisConnectionFactory redisConnectionFactory, ConsumerHalt consumerHalt) {
        Map<String, HealthIndicator> checks = new LinkedHashMap<>();
        checks.put("postgres", new DataSourceHealthIndicator(dataSource));
        checks.put("redis", new RedisHealthIndicator(redisConnectionFactory));
        checks.put("consumer", consumerHalt);
        this.readinessChecks = checks;
    }

    /** For unit tests: readiness checks supplied directly. */
    HealthController(Map<String, HealthIndicator> readinessChecks) {
        this.readinessChecks = readinessChecks;
    }

    @GetMapping("/healthz")
    public Map<String, String> healthz() {
        return Map.of("status", "ok");
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, Object>> readyz() {
        Map<String, String> failures = new LinkedHashMap<>();
        readinessChecks.forEach((name, check) -> {
            Health health = check.health();
            if (!Status.UP.equals(health.getStatus())) {
                failures.put(name, describe(health));
            }
        });
        if (!failures.isEmpty()) {
            log.atError().addKeyValue("failures", failures).log("readiness check failed");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("status", "not_ready", "failures", failures));
        }
        return ResponseEntity.ok(Map.of("status", "ready"));
    }

    private static String describe(Health health) {
        Object error = health.getDetails().get("error");
        return error != null ? error.toString() : health.getStatus().getCode();
    }
}
