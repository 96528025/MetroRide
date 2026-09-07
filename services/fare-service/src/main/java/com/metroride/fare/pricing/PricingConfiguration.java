package com.metroride.fare.pricing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class PricingConfiguration {

    /** The calculator has no Spring dependency itself; this only hands it the bound rate card. */
    @Bean
    FareCalculator fareCalculator(FareProperties rates) {
        return new FareCalculator(rates);
    }
}
