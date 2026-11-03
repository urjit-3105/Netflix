package com.netflix.resilience.runner;

import com.netflix.resilience.model.StreamResponse;
import com.netflix.resilience.service.StreamingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrency & Stress Test Runner.
 * Can be activated with Spring profile: -Dspring.profiles.active=loadtest
 * or triggered programmatically to evaluate resilience mechanics under load.
 */
@Component
@Profile("loadtest")
public class LoadSimulationRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadSimulationRunner.class);
    private final StreamingService streamingService;

    public LoadSimulationRunner(StreamingService streamingService) {
        this.streamingService = streamingService;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("======================================================================");
        log.info("🚀 STARTING NETFLIX RESILIENCE LOAD SIMULATION RUNNER");
        log.info("======================================================================");

        int totalRequests = 30;
        int concurrency = 10;
        String clientId = "stress-test-subscriber";

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(totalRequests);

        AtomicInteger okCount = new AtomicInteger();
        AtomicInteger fallbackCount = new AtomicInteger();
        AtomicInteger throttledCount = new AtomicInteger();

        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= totalRequests; i++) {
            final int requestId = i;
            // Inject 60% failure rate for odd requests to trip the circuit
            final boolean forceFailure = (requestId % 2 == 1);

            executor.submit(() -> {
                try {
                    StreamResponse response = streamingService.streamContent(
                            "movie-chunk-" + requestId,
                            clientId,
                            forceFailure,
                            0,
                            0
                    );

                    switch (response.getStatus()) {
                        case "OK" -> {
                            okCount.incrementAndGet();
                            log.info("🟢 Req #{}: OK [4K UHD] | Circuit: {}", requestId, response.getCircuitState());
                        }
                        case "THROTTLED" -> {
                            throttledCount.incrementAndGet();
                            log.warn("🟡 Req #{}: THROTTLED [Rate Limiter Engaged] | Circuit: {}", requestId, response.getCircuitState());
                        }
                        case "FALLBACK" -> {
                            fallbackCount.incrementAndGet();
                            log.error("🔴 Req #{}: FALLBACK [Edge Degraded 720p] | Circuit: {}", requestId, response.getCircuitState());
                        }
                    }
                } catch (Exception e) {
                    fallbackCount.incrementAndGet();
                    log.error("💥 Req #{}: Exception: {}", requestId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long totalTime = System.currentTimeMillis() - startTime;
        log.info("======================================================================");
        log.info("📊 LOAD TEST EXECUTION SUMMARY");
        log.info("======================================================================");
        log.info("Total Requests Executed : {}", totalRequests);
        log.info("Concurrency Level       : {} parallel threads", concurrency);
        log.info("Total Duration          : {} ms", totalTime);
        log.info("Successful Responses    : {}", okCount.get());
        log.info("Circuit Fallbacks       : {}", fallbackCount.get());
        log.info("Rate Limited / Throttled: {}", throttledCount.get());
        log.info("Final Circuit State     : {}", streamingService.getCircuitBreaker().getState());
        log.info("======================================================================");
    }
}

