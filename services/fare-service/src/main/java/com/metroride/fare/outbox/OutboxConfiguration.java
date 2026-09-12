package com.metroride.fare.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
class OutboxConfiguration {

    @Bean
    OutboxRepository outboxRepository(
            DataSource dataSource,
            ObjectMapper mapper,
            @Value("${metroride.postgres.timeout-seconds}") int statementTimeoutSeconds) {
        return new OutboxRepository(dataSource, mapper, statementTimeoutSeconds);
    }

    /**
     * The relay's transaction: no overall timeout on purpose. Each statement in it is bounded by
     * the repository's query timeout and each {@code XADD} by the Redis command timeout, as in the
     * Go relay; a batch-wide budget would cancel the update that records a slow publish's failure
     * and roll the backoff back with it.
     */
    @Bean
    TransactionOperations outboxTransaction(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }
}
