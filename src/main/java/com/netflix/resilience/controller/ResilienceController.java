package com.netflix.resilience.controller;

import com.netflix.resilience.config.ResilienceConfig;
import com.netflix.resilience.model.HealthStatusResponse;
import com.netflix.resilience.model.StreamResponse;
import com.netflix.resilience.service.StreamingService;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/v1/content")
public class ResilienceController {

    private final StreamingService streamingService;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiterRegistry rateLimiterRegistry;
    private final Bulkhead bulkhead;
    private final Retry retry;
    private final ResilienceConfig resilienceConfig;

    public ResilienceController(StreamingService streamingService,
                                CircuitBreaker streamingServiceCircuitBreaker,
                                RateLimiterRegistry rateLimiterRegistry,
                                Bulkhead streamingBulkhead,
                                Retry streamingRetry,
                                ResilienceConfig resilienceConfig) {
        this.streamingService = streamingService;
        this.circuitBreaker = streamingServiceCircuitBreaker;
        this.rateLimiterRegistry = rateLimiterRegistry;
        this.bulkhead = streamingBulkhead;
        this.retry = streamingRetry;
        this.resilienceConfig = resilienceConfig;
    }

    /**
     * Primary Streaming Endpoint protected by Circuit Breaker, Rate Limiter, Bulkhead, and Retry.
     */
    @GetMapping("/stream")
    public ResponseEntity<StreamResponse> streamContent(
            @RequestParam(name = "contentId", defaultValue = "stranger-things-s5") String contentId,
            @RequestParam(name = "clientId", defaultValue = "subscriber-default") String clientId,
            @RequestParam(name = "fail", defaultValue = "false") boolean fail,
            @RequestParam(name = "latency", defaultValue = "0") long latencyMs,
            @RequestParam(name = "randomFail", defaultValue = "0") int randomFailureRate) {

        StreamResponse response = streamingService.streamContent(contentId, clientId, fail, latencyMs, randomFailureRate);

        if ("THROTTLED".equals(response.getStatus())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(response);
        }
        if ("BULKHEAD_FULL".equals(response.getStatus())) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
        }
        return ResponseEntity.ok(response);
    }

    /**
     * Health Status & Telemetry Endpoint exposing live metrics across CircuitBreaker, RateLimiter, Bulkhead, and Retry.
     */
    @GetMapping("/health-status")
    public ResponseEntity<HealthStatusResponse> getHealthStatus(
            @RequestParam(name = "clientId", defaultValue = "subscriber-default") String clientId) {

        Map<String, Object> cbMetrics = new LinkedHashMap<>();
        cbMetrics.put("name", circuitBreaker.getName());
        cbMetrics.put("state", circuitBreaker.getState().name());
        cbMetrics.put("failureRateThreshold", circuitBreaker.getCircuitBreakerConfig().getFailureRateThreshold() + "%");
        cbMetrics.put("slowCallRateThreshold", circuitBreaker.getCircuitBreakerConfig().getSlowCallRateThreshold() + "%");
        cbMetrics.put("slowCallDurationThresholdMs", circuitBreaker.getCircuitBreakerConfig().getSlowCallDurationThreshold().toMillis());
        cbMetrics.put("currentFailureRate", circuitBreaker.getMetrics().getFailureRate() + "%");
        cbMetrics.put("currentSlowCallRate", circuitBreaker.getMetrics().getSlowCallRate() + "%");
        cbMetrics.put("bufferedCalls", circuitBreaker.getMetrics().getNumberOfBufferedCalls());
        cbMetrics.put("successfulCalls", circuitBreaker.getMetrics().getNumberOfSuccessfulCalls());
        cbMetrics.put("failedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls());
        cbMetrics.put("notPermittedCalls", circuitBreaker.getMetrics().getNumberOfNotPermittedCalls());

        String rateLimiterKey = (clientId != null && !clientId.isBlank())
                ? "rateLimiter-" + clientId.trim()
                : ResilienceConfig.DEFAULT_RATE_LIMITER_NAME;
        RateLimiter rl = rateLimiterRegistry.rateLimiter(rateLimiterKey);

        Map<String, Object> rlMetrics = new LinkedHashMap<>();
        rlMetrics.put("name", rl.getName());
        rlMetrics.put("availablePermissions", rl.getMetrics().getAvailablePermissions());
        rlMetrics.put("numberOfWaitingThreads", rl.getMetrics().getNumberOfWaitingThreads());
        rlMetrics.put("limitForPeriod", rl.getRateLimiterConfig().getLimitForPeriod());
        rlMetrics.put("limitRefreshPeriodMs", rl.getRateLimiterConfig().getLimitRefreshPeriod().toMillis());

        Map<String, Object> bhMetrics = new LinkedHashMap<>();
        bhMetrics.put("name", bulkhead.getName());
        bhMetrics.put("availableConcurrentCalls", bulkhead.getMetrics().getAvailableConcurrentCalls());
        bhMetrics.put("maxConcurrentCalls", bulkhead.getBulkheadConfig().getMaxConcurrentCalls());

        Map<String, Object> retryMetrics = new LinkedHashMap<>();
        retryMetrics.put("name", retry.getName());
        retryMetrics.put("maxAttempts", retry.getRetryConfig().getMaxAttempts());
        retryMetrics.put("successfulCallsWithoutRetry", retry.getMetrics().getNumberOfSuccessfulCallsWithoutRetryAttempt());
        retryMetrics.put("successfulCallsWithRetry", retry.getMetrics().getNumberOfSuccessfulCallsWithRetryAttempt());
        retryMetrics.put("failedCallsWithoutRetry", retry.getMetrics().getNumberOfFailedCallsWithoutRetryAttempt());
        retryMetrics.put("failedCallsWithRetry", retry.getMetrics().getNumberOfFailedCallsWithRetryAttempt());

        String overallHealth = circuitBreaker.getState() == CircuitBreaker.State.OPEN
                ? "DEGRADED (Circuit Breaker OPEN - Serving Cached Fallbacks)"
                : "HEALTHY (Streaming Service UP - Holy Trinity Active)";

        HealthStatusResponse healthStatus = HealthStatusResponse.builder()
                .serviceName("netflix-resilience-microservice")
                .overallHealth(overallHealth)
                .circuitBreaker(cbMetrics)
                .rateLimiter(rlMetrics)
                .bulkhead(bhMetrics)
                .retry(retryMetrics)
                .recentResilienceEvents(resilienceConfig.getRecentResilienceEvents())
                .timestamp(System.currentTimeMillis())
                .build();

        return ResponseEntity.ok(healthStatus);
    }

    /**
     * Load & Concurrency Stress Test Endpoint.
     */
    @PostMapping("/simulate-load")
    public ResponseEntity<Map<String, Object>> simulateLoad(
            @RequestParam(name = "totalRequests", defaultValue = "20") int totalRequests,
            @RequestParam(name = "concurrency", defaultValue = "10") int concurrency,
            @RequestParam(name = "failPercentage", defaultValue = "60") int failPercentage,
            @RequestParam(name = "clientId", defaultValue = "load-tester-client") String clientId) throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(totalRequests);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger fallbackCount = new AtomicInteger();
        AtomicInteger throttledCount = new AtomicInteger();
        AtomicInteger bulkheadCount = new AtomicInteger();
        List<String> logs = new CopyOnWriteArrayList<>();

        long start = System.currentTimeMillis();

        for (int i = 1; i <= totalRequests; i++) {
            final int requestId = i;
            final boolean shouldFail = (requestId % 100) <= failPercentage;
            executor.submit(() -> {
                try {
                    StreamResponse res = streamingService.streamContent(
                            "movie-" + requestId,
                            clientId,
                            shouldFail,
                            0,
                            0
                    );

                    switch (res.getStatus()) {
                        case "OK" -> successCount.incrementAndGet();
                        case "THROTTLED" -> throttledCount.incrementAndGet();
                        case "BULKHEAD_FULL" -> bulkheadCount.incrementAndGet();
                        default -> fallbackCount.incrementAndGet();
                    }
                    logs.add(String.format("Req #%02d -> Status: %s | Quality: %s | Circuit: %s",
                            requestId, res.getStatus(), res.getQuality(), res.getCircuitState()));
                } catch (Exception e) {
                    fallbackCount.incrementAndGet();
                    logs.add("Req #" + requestId + " -> Error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(15, TimeUnit.SECONDS);
        executor.shutdown();
        long totalDurationMs = System.currentTimeMillis() - start;

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalRequestsDispatched", totalRequests);
        summary.put("concurrencyLevel", concurrency);
        summary.put("simulatedFailureRate", failPercentage + "%");
        summary.put("successfulResponses", successCount.get());
        summary.put("circuitBreakerFallbacks", fallbackCount.get());
        summary.put("rateLimitedThrottled", throttledCount.get());
        summary.put("bulkheadSaturated", bulkheadCount.get());
        summary.put("finalCircuitState", circuitBreaker.getState().name());
        summary.put("totalExecutionTimeMs", totalDurationMs);
        summary.put("requestLogsSample", logs.stream().limit(15).toList());

        return ResponseEntity.ok(summary);
    }
}
