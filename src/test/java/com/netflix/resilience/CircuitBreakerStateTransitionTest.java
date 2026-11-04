package com.netflix.resilience;

import com.netflix.resilience.client.DownstreamStreamingClient;
import com.netflix.resilience.config.ResilienceConfig;
import com.netflix.resilience.exception.DownstreamServiceException;
import com.netflix.resilience.model.StreamResponse;
import com.netflix.resilience.service.StreamingService;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CircuitBreakerStateTransitionTest {

    private TestStubDownstreamClient downstreamClient;
    private CircuitBreaker circuitBreaker;
    private RateLimiterRegistry rateLimiterRegistry;
    private StreamingService streamingService;

    @BeforeEach
    void setUp() {
        // Fast-reacting test configuration: 5-call sliding window, 50% failure rate threshold, 1s wait in open state
        CircuitBreakerConfig testConfig = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .permittedNumberOfCallsInHalfOpenState(2)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        CircuitBreakerRegistry cbRegistry = CircuitBreakerRegistry.of(testConfig);
        this.circuitBreaker = cbRegistry.circuitBreaker("testStreamingCircuitBreaker");

        RateLimiterConfig rlConfig = RateLimiterConfig.custom()
                .limitForPeriod(100)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ZERO)
                .build();
        this.rateLimiterRegistry = RateLimiterRegistry.of(rlConfig);

        Bulkhead bulkhead = BulkheadRegistry.of(BulkheadConfig.ofDefaults()).bulkhead("cbTestBulkhead");
        // maxAttempts: 1 to isolate circuit breaker testing without retry inflation
        Retry retry = RetryRegistry.of(RetryConfig.custom().maxAttempts(1).build()).retry("cbTestRetry");

        this.downstreamClient = new TestStubDownstreamClient();
        ResilienceConfig resilienceConfig = new ResilienceConfig();
        this.streamingService = new StreamingService(downstreamClient, circuitBreaker, rateLimiterRegistry, bulkhead, retry, resilienceConfig);
    }

    @Test
    @DisplayName("Should operate normally in CLOSED state when downstream calls succeed")
    void testClosedStateWhenSuccessful() {
        downstreamClient.setShouldThrow(false);

        for (int i = 0; i < 5; i++) {
            StreamResponse response = streamingService.streamContent("movie-1", "client-1", false, 0, 0);
            assertEquals("OK", response.getStatus());
            assertEquals(CircuitBreaker.State.CLOSED.name(), response.getCircuitState());
        }

        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
        assertEquals(0.0f, circuitBreaker.getMetrics().getFailureRate());
        assertEquals(5, downstreamClient.getInvocationCount());
    }

    @Test
    @DisplayName("Should trip circuit from CLOSED to OPEN on downstream failure threshold breach and invoke fallback")
    void testTransitionFromClosedToOpen() {
        downstreamClient.setShouldThrow(true);

        // Fire 5 failing requests to meet minimumNumberOfCalls (5) with 100% failure rate (> 50% threshold)
        for (int i = 0; i < 5; i++) {
            StreamResponse response = streamingService.streamContent("movie-" + i, "client-1", true, 0, 0);
            assertEquals("FALLBACK", response.getStatus());
            assertTrue(response.getMessage().contains("Downstream CDN unavailable"));
        }

        // Circuit must now be OPEN
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
        assertEquals(5, downstreamClient.getInvocationCount());

        // In OPEN state, verify downstream client is never called again (fail-fast active)
        StreamResponse blockedCall = streamingService.streamContent("movie-fast-fail", "client-1", false, 0, 0);
        assertEquals("FALLBACK", blockedCall.getStatus());
        assertTrue(blockedCall.getMessage().contains("Circuit Breaker OPEN"));
        // Invocation count must remain 5
        assertEquals(5, downstreamClient.getInvocationCount());
    }

    @Test
    @DisplayName("Should transition from OPEN to HALF_OPEN and recover back to CLOSED on successful recovery calls")
    void testTransitionFromOpenToHalfOpenAndRecover() throws InterruptedException {
        downstreamClient.setShouldThrow(true);

        // Trip to OPEN
        for (int i = 0; i < 5; i++) {
            streamingService.streamContent("movie-" + i, "client-1", true, 0, 0);
        }
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());

        // Wait for waitDurationInOpenState (1000ms) to elapse
        Thread.sleep(1200);

        // Prepare healthy responses for recovery calls
        downstreamClient.setShouldThrow(false);

        // First trial call in HALF_OPEN state
        StreamResponse trial1 = streamingService.streamContent("movie-rec-1", "client-1", false, 0, 0);
        assertEquals("OK", trial1.getStatus());

        // Second trial call in HALF_OPEN state
        StreamResponse trial2 = streamingService.streamContent("movie-rec-2", "client-1", false, 0, 0);
        assertEquals("OK", trial2.getStatus());

        // After 2 successful calls in HALF_OPEN (permittedNumberOfCallsInHalfOpenState = 2), circuit closes
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
    }

    static class TestStubDownstreamClient implements DownstreamStreamingClient {
        private boolean shouldThrow = false;
        private final AtomicInteger invocationCount = new AtomicInteger();

        public void setShouldThrow(boolean shouldThrow) {
            this.shouldThrow = shouldThrow;
        }

        public int getInvocationCount() {
            return invocationCount.get();
        }

        @Override
        public StreamResponse fetchStreamFromDownstream(String contentId, String clientId, boolean simulateError, long simulatedLatencyMs, int randomFailureRatePercentage) {
            invocationCount.incrementAndGet();
            if (shouldThrow || simulateError) {
                throw new DownstreamServiceException("503 Downstream Service Unavailable");
            }
            return StreamResponse.builder()
                    .contentId(contentId)
                    .clientId(clientId)
                    .title("Stranger Things")
                    .quality("4K UHD Dolby Vision")
                    .bitrate("18.5 Mbps")
                    .status("OK")
                    .message("Stream active")
                    .build();
        }
    }
}
