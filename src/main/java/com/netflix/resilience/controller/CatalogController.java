package com.netflix.resilience.controller;

import com.netflix.resilience.service.CatalogService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/v1/catalog")
public class CatalogController {

    private final CatalogService catalogService;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RateLimiterRegistry rateLimiterRegistry;

    public CatalogController(CatalogService catalogService,
                             CircuitBreakerRegistry circuitBreakerRegistry,
                             RateLimiterRegistry rateLimiterRegistry) {
        this.catalogService = catalogService;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.rateLimiterRegistry = rateLimiterRegistry;
    }

    /**
     * Fetch video metadata with simulation parameters.
     */
    @GetMapping("/video/{id}")
    public ResponseEntity<Map<String, Object>> getVideo(
            @PathVariable("id") String id,
            @RequestParam(name = "fail", defaultValue = "false") boolean fail,
            @RequestParam(name = "delay", defaultValue = "0") long delayMs) {

        String result = catalogService.getVideoMetadata(id, fail, delayMs);

        Map<String, Object> response = new HashMap<>();
        response.put("result", result);
        response.put("circuitState", getCircuitBreakerState());
        return ResponseEntity.ok(response);
    }

    /**
     * Endpoint to check current Circuit Breaker & Rate Limiter status.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getResilienceStatus() {
        Map<String, Object> status = new HashMap<>();

        CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker("catalogService");
        status.put("circuitBreakerName", cb.getName());
        status.put("state", cb.getState().name());
        status.put("failureRate", cb.getMetrics().getFailureRate());
        status.put("slowCallRate", cb.getMetrics().getSlowCallRate());
        status.put("numberOfBufferedCalls", cb.getMetrics().getNumberOfBufferedCalls());
        status.put("numberOfFailedCalls", cb.getMetrics().getNumberOfFailedCalls());
        status.put("numberOfSuccessfulCalls", cb.getMetrics().getNumberOfSuccessfulCalls());

        RateLimiter rl = rateLimiterRegistry.rateLimiter("streamLimiter");
        status.put("rateLimiterAvailablePermissions", rl.getMetrics().getAvailablePermissions());
        status.put("rateLimiterNumberOfWaitingThreads", rl.getMetrics().getNumberOfWaitingThreads());

        return ResponseEntity.ok(status);
    }

    /**
     * Benchmark endpoint that triggers concurrent requests to simulate traffic load
     * and observe circuit transitions (CLOSED -> OPEN -> HALF_OPEN).
     */
    @PostMapping("/simulate-load")
    public ResponseEntity<Map<String, Object>> simulateConcurrentLoad(
            @RequestParam(name = "totalRequests", defaultValue = "10") int totalRequests,
            @RequestParam(name = "failRatePercent", defaultValue = "70") int failRatePercent,
            @RequestParam(name = "concurrency", defaultValue = "5") int concurrency) throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger fallbackCount = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(totalRequests);

        for (int i = 0; i < totalRequests; i++) {
            final int index = i;
            final boolean shouldFail = (index % 100) < failRatePercent;
            executor.submit(() -> {
                try {
                    String res = catalogService.getVideoMetadata("movie-" + index, shouldFail, 0);
                    if (res.contains("[CIRCUIT BREAKER FALLBACK]") || res.contains("[RATE LIMIT FALLBACK]")) {
                        fallbackCount.incrementAndGet();
                    } else {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    fallbackCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        Map<String, Object> summary = new HashMap<>();
        summary.put("totalRequests", totalRequests);
        summary.put("successCount", successCount.get());
        summary.put("fallbackCount", fallbackCount.get());
        summary.put("finalCircuitState", getCircuitBreakerState());

        return ResponseEntity.ok(summary);
    }

    private String getCircuitBreakerState() {
        return circuitBreakerRegistry.circuitBreaker("catalogService").getState().name();
    }
}
