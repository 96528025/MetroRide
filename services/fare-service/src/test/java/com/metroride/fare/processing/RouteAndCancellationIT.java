package com.metroride.fare.processing;

import static org.assertj.core.api.Assertions.*;
import com.metroride.fare.IntegrationTestSupport;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.pricing.FareCalculator;
import com.metroride.fare.pricing.FareProperties;
import com.metroride.fare.pricing.FareQuoteException;
import com.metroride.fare.outbox.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class RouteAndCancellationIT extends IntegrationTestSupport {
    @Autowired ProcessedEventRecorder recorder;
    @Autowired ProcessedEventRepository processed;
    @Autowired LedgerRepository ledger;
    @Autowired EnvelopeCodec codec;
    @Autowired OutboxRepository outbox;
    @Autowired ObjectMapper mapper;
    @Autowired RideContextRepository context;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactions;

    private Envelope event(String type, String ride, Map<String, Object> extra) {
        var payload = mapper.createObjectNode().put("ride_id", ride).put("rider_id", "rider")
                .put("driver_id", "driver").put("assignment_id", "assignment");
        extra.forEach((key, value) -> payload.set(key, mapper.valueToTree(value)));
        return new Envelope(UUID.randomUUID().toString(), type, "test", ride, Instant.now(), payload);
    }
    private Envelope assignment(String ride) {
        return event("ride_assigned", ride, Map.of("schema_version", 2,
                "distance_km", 99, "eta_seconds", 9999,
                "trip_distance_km", new BigDecimal("8.5"), "trip_duration_seconds", new BigDecimal("920.5"),
                "route_provider", "test-fixture", "route_calculated_at", "2026-09-15T00:00:00Z"));
    }
    private void record(Envelope e) { recorder.record("test.routes", e); }
    private int entries(String ride, String kind) {
        return jdbc.queryForObject("select count(*) from fare.journal_entries where ride_id=? and kind=?", Integer.class, ride, kind);
    }
    private BigDecimal balance(String ride, String account) {
        return jdbc.queryForObject("select coalesce(sum(p.amount),0) from fare.postings p join fare.journal_entries j on j.id=p.journal_entry_id where j.ride_id=? and p.account=?", BigDecimal.class, ride, account);
    }
    @Test void passengerRoutePricesTheQuoteAndFreezesTheRateCardAndDriverSplit() {
        String ride=UUID.randomUUID().toString(); record(assignment(ride));
        assertThat(balance(ride,"rider_receivable")).isEqualByComparingTo("17.30");
        assertThat(jdbc.queryForObject("select pricing_version from fare.quote_context where ride_id=?",String.class,ride)).isEqualTo("passenger-road-v2");
        FareProperties changed=new FareProperties(new BigDecimal("100"),new BigDecimal("99"),new BigDecimal("99"),new BigDecimal("0.10"));
        ProcessedEventRecorder restarted=new ProcessedEventRecorder(processed,ledger,codec,new FareCalculator(changed),changed,outbox,mapper,Clock.systemUTC(),context);
        new TransactionTemplate(transactions).executeWithoutResult(status -> restarted.record("test.routes",event("ride_completed",ride,Map.of())));
        assertThat(balance(ride,"rider_receivable")).isEqualByComparingTo("17.30");
        assertThat(balance(ride,"driver_payable")).isEqualByComparingTo("-13.84");
        assertThat(balance(ride,"fare_hold")).isZero();
    }
    @Test void cancellationReversesTheHoldExactlyOnceAndPreventsSettlement() {
        String ride=UUID.randomUUID().toString(); record(assignment(ride));
        Envelope cancel=event("ride_cancelled",ride,Map.of()); record(cancel); record(cancel);
        record(event("ride_cancelled",ride,Map.of()));
        assertThat(entries(ride,"cancellation_reversal")).isEqualTo(1);
        assertThat(balance(ride,"rider_receivable")).isZero();
        assertThat(balance(ride,"fare_hold")).isZero();
        assertThatThrownBy(()->record(event("ride_completed",ride,Map.of()))).isInstanceOf(SettlementException.class);
        assertThat(entries(ride,"settlement")).isZero();
    }
    @Test void cancellationBeforeAssignmentCreatesNoHold() {
        String ride=UUID.randomUUID().toString(); record(event("ride_cancelled",ride,Map.of()));
        record(assignment(ride)); record(assignment(ride));
        assertThat(entries(ride,"quote_hold")).isZero();
        assertThat(jdbc.queryForObject("select state from fare.ride_state where ride_id=?",String.class,ride)).isEqualTo("cancelled");
    }
    @Test void oldOrIncompleteRoutePayloadCannotSilentlyPriceDriverApproach() {
        for (Map<String,Object> fields : java.util.List.of(Map.<String,Object>of("distance_km",1,"eta_seconds",60),
                Map.<String,Object>of("schema_version",2,"route_provider","test","route_calculated_at","2026-09-15T00:00:00Z","trip_distance_km",1))) {
            String ride=UUID.randomUUID().toString();
            assertThatThrownBy(()->record(event("ride_assigned",ride,fields))).isInstanceOf(FareQuoteException.class);
            assertThat(entries(ride,"quote_hold")).isZero();
        }
    }
    @Test void completionAndCancellationHaveOneDurableWinner() throws Exception {
        String ride=UUID.randomUUID().toString(); record(assignment(ride));
        CountDownLatch start=new CountDownLatch(1);
        try (ExecutorService pool=Executors.newFixedThreadPool(2)) {
            Callable<Boolean> complete=()->{start.await();try {record(event("ride_completed",ride,Map.of()));return true;}catch(SettlementException expected){return false;}};
            Callable<Boolean> cancel=()->{start.await();try {record(event("ride_cancelled",ride,Map.of()));return true;}catch(SettlementException expected){return false;}};
            Future<Boolean> a=pool.submit(complete), b=pool.submit(cancel); start.countDown();
            assertThat(a.get(10,TimeUnit.SECONDS)^b.get(10,TimeUnit.SECONDS)).isTrue();
        }
        assertThat(entries(ride,"settlement")+entries(ride,"cancellation_reversal")).isEqualTo(1);
        assertThat(balance(ride,"fare_hold")).isZero();
    }
}
