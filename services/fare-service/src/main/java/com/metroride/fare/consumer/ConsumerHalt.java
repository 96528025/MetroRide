package com.metroride.fare.consumer;

import com.metroride.fare.FareServiceApplication;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Whether the stream consumer has stopped on a {@link FailureClass#FATAL} failure, and why. Set
 * once by the failure handler, read by the consumer loop (which exits) and by {@code /readyz}
 * (which reports {@code consumer} as a failed dependency), and exposed as the gauge
 * {@code metroride_fare_consumer_halted}. A halt is not cleared at runtime: the deployment is
 * wrong, and the entries left pending are reclaimed by the next start after it is fixed.
 */
@Component
public class ConsumerHalt implements HealthIndicator {

    private final AtomicReference<String> reason = new AtomicReference<>();

    public ConsumerHalt(MeterRegistry meterRegistry) {
        Gauge.builder("metroride.fare.consumer.halted", this, halt -> halt.isHalted() ? 1 : 0)
                .tag("service", FareServiceApplication.SERVICE_NAME)
                .register(meterRegistry);
    }

    /** Records the first reason only; later calls keep the original. */
    public void halt(String why) {
        reason.compareAndSet(null, why);
    }

    public boolean isHalted() {
        return reason.get() != null;
    }

    public Optional<String> reason() {
        return Optional.ofNullable(reason.get());
    }

    @Override
    public Health health() {
        String why = reason.get();
        return why == null ? Health.up().build() : Health.down().withDetail("error", why).build();
    }
}
