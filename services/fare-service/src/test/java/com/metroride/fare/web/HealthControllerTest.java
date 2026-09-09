package com.metroride.fare.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.metroride.fare.consumer.ConsumerHalt;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
    void notReadyOnceTheConsumerHaltedWithTheReasonAsTheMessage() {
        ConsumerHalt halt = new ConsumerHalt(new SimpleMeterRegistry());
        Map<String, HealthIndicator> checks = checks(Health.up().build(), Health.up().build());
        checks.put("consumer", halt);
        HealthController controller = new HealthController(checks);
        assertThat(controller.readyz().getStatusCode()).isEqualTo(HttpStatus.OK);

        halt.halt("fatal failure handling events.ride.assignments/1-0: BadSqlGrammarException: column kind does not exist");

        ResponseEntity<Map<String, Object>> response = controller.readyz();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().get("failures")).isEqualTo(Map.of("consumer",
                "fatal failure handling events.ride.assignments/1-0: BadSqlGrammarException: column kind does not exist"));
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
