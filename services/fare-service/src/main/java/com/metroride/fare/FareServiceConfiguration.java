package com.metroride.fare;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class FareServiceConfiguration {

    /** Injected wherever "now" is stored so tests can pin time; the Go services store {@code time.Now().UTC()}. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
