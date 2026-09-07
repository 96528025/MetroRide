package com.metroride.fare.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class HealthControllerTest {

    @Test
    void readyWhenEveryDependencyIsUp() {
        HealthController controller = new HealthController(checks(Health.up().build(), Health.up().build()));

        ResponseEntity<Map<String, Object>> response = controller.readyz();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsExactly(Map.entry("status", "ready"));
    }

    @Test
    void notReadyListsEachFailingDependencyLikeTheGoServices() {
        Health postgresDown = Health.down().withDetail("error", "PSQLException: connection refused").build();
        HealthController controller = new HealthController(checks(postgresDown, Health.up().build()));

        ResponseEntity<Map<String, Object>> response = controller.readyz();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).containsEntry("status", "not_ready");
        assertThat(response.getBody().get("failures"))
                .isEqualTo(Map.of("postgres", "PSQLException: connection refused"));
    }

    @Test
    void livenessIsStatic() {
        HealthController controller = new HealthController(checks(Health.down().build(), Health.down().build()));

        assertThat(controller.healthz()).isEqualTo(Map.of("status", "ok"));
    }

    private static Map<String, HealthIndicator> checks(Health postgres, Health redis) {
        Map<String, HealthIndicator> checks = new LinkedHashMap<>();
        checks.put("postgres", () -> postgres);
        checks.put("redis", () -> redis);
        return checks;
    }
}
