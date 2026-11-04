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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BulkheadAndRetryResilienceTest {

    private TestStubDownstreamClient downstreamClient;
    private Bulkhead bulkhead;
    private Retry retry;
    private StreamingService streamingService;

    @BeforeEach
    void setUp() {
        // Bulkhead configured with small capacity: max 2 concurrent calls
        BulkheadConfig bhConfig = BulkheadConfig.custom()
                .maxConcurrentCalls(2)
                .maxWaitDuration(Duration.ofMillis(10))
                .build();
        this.bulkhead = BulkheadRegistry.of(bhConfig).bulkhead("testBulkhead");

        // Retry configured: 3 attempts with 50ms wait
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(50))
                .retryExceptions(DownstreamServiceException.class)
                .build();
        this.retry = RetryRegistry.of(retryConfig).retry("testRetry");

        CircuitBreaker cb = CircuitBreakerRegistry.ofDefaults().circuitBreaker("testCb");
        RateLimiterRegistry rlRegistry = RateLimiterRegistry.of(RateLimiterConfig.ofDefaults());
        ResilienceConfig resilienceConfig = new ResilienceConfig();
        this.downstreamClient = new TestStubDownstreamClient();

        this.streamingService = new StreamingService(downstreamClient, cb, rlRegistry, bulkhead, retry, resilienceConfig);
    }

    @Test
    @DisplayName("Should automatically retry transient downstream failures up to 3 times before succeeding")
    void testRetryRecoversTransientFailure() {
        // Downstream fails 2 times, then succeeds on 3rd attempt
        downstreamClient.setFailuresBeforeSuccess(2);

        StreamResponse response = streamingService.streamContent("movie-transient", "sub-1", false, 0, 0);

        assertEquals("OK", response.getStatus());
        assertEquals(3, downstreamClient.getInvocationCount());
        assertEquals(1, retry.getMetrics().getNumberOfSuccessfulCallsWithRetryAttempt());
    }

    @Test
    @DisplayName("Should exhaust retries on permanent downstream outage and gracefully fall back")
    void testRetryExhaustionTriggersFallback() {
        // Downstream permanently fails
        downstreamClient.setFailuresBeforeSuccess(99);

        StreamResponse response = streamingService.streamContent("movie-fail", "sub-1", true, 0, 0);

        assertEquals("FALLBACK", response.getStatus());
        assertEquals(3, downstreamClient.getInvocationCount());
        assertEquals(1, retry.getMetrics().getNumberOfFailedCallsWithRetryAttempt());
    }

    @Test
    @DisplayName("Should isolate and reject concurrent requests when Bulkhead limit is saturated")
    void testBulkheadRejectionUnderConcurrency() throws InterruptedException {
        int threads = 6;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        AtomicInteger bulkheadFullCount = new AtomicInteger();
        AtomicInteger okCount = new AtomicInteger();

        // Downstream has delay to hold execution slots in bulkhead
        downstreamClient.setSimulatedDelayMs(300);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    StreamResponse res = streamingService.streamContent("movie-bulkhead", "sub-bulkhead", false, 0, 0);
                    if ("BULKHEAD_FULL".equals(res.getStatus())) {
                        bulkheadFullCount.incrementAndGet();
                    } else if ("OK".equals(res.getStatus())) {
                        okCount.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Unleash all threads simultaneously
        startLatch.countDown();
        doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Bulkhead capacity is 2; out of 6 concurrent threads, excess requests must be rejected with BULKHEAD_FULL
        assertTrue(bulkheadFullCount.get() > 0, "Expected bulkhead saturation rejections but got 0");
        assertTrue(okCount.get() <= 2, "OK count should not exceed bulkhead max concurrent capacity of 2");
    }

    static class TestStubDownstreamClient implements DownstreamStreamingClient {
        private final AtomicInteger invocationCount = new AtomicInteger();
        private int failuresBeforeSuccess = 0;
        private long simulatedDelayMs = 0;

        public void setFailuresBeforeSuccess(int failuresBeforeSuccess) {
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        public void setSimulatedDelayMs(long simulatedDelayMs) {
            this.simulatedDelayMs = simulatedDelayMs;
        }

        public int getInvocationCount() {
            return invocationCount.get();
        }

        @Override
        public StreamResponse fetchStreamFromDownstream(String contentId, String clientId, boolean simulateError, long simulatedLatencyMs, int randomFailureRatePercentage) {
            int current = invocationCount.incrementAndGet();

            if (simulatedDelayMs > 0) {
                try {
                    Thread.sleep(simulatedDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            if (simulateError || current <= failuresBeforeSuccess) {
                throw new DownstreamServiceException("Downstream Transient Error (attempt " + current + ")");
            }

            return StreamResponse.builder()
                    .contentId(contentId)
                    .clientId(clientId)
                    .title("Stranger Things")
                    .quality("4K UHD")
                    .status("OK")
                    .build();
        }
    }
}
