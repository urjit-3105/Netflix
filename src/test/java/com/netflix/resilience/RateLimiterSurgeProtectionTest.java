package com.netflix.resilience;

import com.netflix.resilience.client.DownstreamStreamingClient;
import com.netflix.resilience.config.ResilienceConfig;
import com.netflix.resilience.model.StreamResponse;
import com.netflix.resilience.service.StreamingService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterSurgeProtectionTest {

    private TestStubDownstreamClient downstreamClient;
    private StreamingService streamingService;

    @BeforeEach
    void setUp() {
        // Configure strict rate limiter: 3 requests per 1 second
        RateLimiterConfig rlConfig = RateLimiterConfig.custom()
                .limitForPeriod(3)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ZERO)
                .build();

        RateLimiterRegistry rateLimiterRegistry = RateLimiterRegistry.of(rlConfig);
        CircuitBreaker circuitBreaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker("rateLimitTestCb");
        ResilienceConfig resilienceConfig = new ResilienceConfig();
        this.downstreamClient = new TestStubDownstreamClient();

        this.streamingService = new StreamingService(downstreamClient, circuitBreaker, rateLimiterRegistry, resilienceConfig);
    }

    @Test
    @DisplayName("Should permit requests within rate limit quota and reject traffic surge with THROTTLED status")
    void testRateLimiterPermitsAndThrottles() {
        // First 3 requests must succeed
        for (int i = 0; i < 3; i++) {
            StreamResponse resp = streamingService.streamContent("movie-1", "burst-client", false, 0, 0);
            assertEquals("OK", resp.getStatus());
        }

        // 4th and 5th requests in same period must be rejected fast
        StreamResponse throttled1 = streamingService.streamContent("movie-1", "burst-client", false, 0, 0);
        assertEquals("THROTTLED", throttled1.getStatus());
        assertTrue(throttled1.getMessage().contains("Sliding-window rate limit exceeded"));

        StreamResponse throttled2 = streamingService.streamContent("movie-1", "burst-client", false, 0, 0);
        assertEquals("THROTTLED", throttled2.getStatus());

        // Verify downstream client was only called 3 times (never called for throttled requests)
        assertEquals(3, downstreamClient.getInvocationCount());
    }

    @Test
    @DisplayName("Should maintain isolated rate limits per client ID so one client does not throttle another")
    void testClientLevelIsolation() {
        // Exhaust client-A's limit of 3
        for (int i = 0; i < 3; i++) {
            assertEquals("OK", streamingService.streamContent("movie-1", "client-A", false, 0, 0).getStatus());
        }
        // Client-A is throttled
        assertEquals("THROTTLED", streamingService.streamContent("movie-1", "client-A", false, 0, 0).getStatus());

        // Client-B should still be completely unaffected and succeed
        StreamResponse clientBResponse = streamingService.streamContent("movie-1", "client-B", false, 0, 0);
        assertEquals("OK", clientBResponse.getStatus());
    }

    static class TestStubDownstreamClient implements DownstreamStreamingClient {
        private final AtomicInteger invocationCount = new AtomicInteger();

        public int getInvocationCount() {
            return invocationCount.get();
        }

        @Override
        public StreamResponse fetchStreamFromDownstream(String contentId, String clientId, boolean simulateError, long simulatedLatencyMs, int randomFailureRatePercentage) {
            invocationCount.incrementAndGet();
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
