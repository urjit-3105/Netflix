package com.netflix.resilience.service;

import com.netflix.resilience.client.DownstreamStreamingClient;
import com.netflix.resilience.config.ResilienceConfig;
import com.netflix.resilience.model.StreamResponse;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

@Service
public class StreamingService {

    private static final Logger log = LoggerFactory.getLogger(StreamingService.class);

    private final DownstreamStreamingClient downstreamClient;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiterRegistry rateLimiterRegistry;
    private final ResilienceConfig resilienceConfig;

    public StreamingService(DownstreamStreamingClient downstreamClient,
                            CircuitBreaker streamingServiceCircuitBreaker,
                            RateLimiterRegistry rateLimiterRegistry,
                            ResilienceConfig resilienceConfig) {
        this.downstreamClient = downstreamClient;
        this.circuitBreaker = streamingServiceCircuitBreaker;
        this.rateLimiterRegistry = rateLimiterRegistry;
        this.resilienceConfig = resilienceConfig;
    }

    /**
     * Executes streaming content retrieval protected by Circuit Breaker and Client-Level Sliding-Window Rate Limiter.
     *
     * @param contentId Content ID (e.g. movie/series identifier)
     * @param clientId Client / Subscriber identifier for rate-limiting
     * @param simulateError Flag to force remote failure
     * @param simulatedLatencyMs Simulated remote latency in milliseconds
     * @param randomFailureRate Percentage chance (0-100) of simulated random remote failure
     * @return StreamResponse with playback manifest, bitrate, quality, and circuit status
     */
    public StreamResponse streamContent(String contentId,
                                        String clientId,
                                        boolean simulateError,
                                        long simulatedLatencyMs,
                                        int randomFailureRate) {

        // Acquire rate limiter dedicated for this client ID (or fallback to default)
        String rateLimiterKey = (clientId != null && !clientId.isBlank())
                ? "rateLimiter-" + clientId.trim()
                : ResilienceConfig.DEFAULT_RATE_LIMITER_NAME;

        RateLimiter clientLimiter = rateLimiterRegistry.rateLimiter(rateLimiterKey);

        // Decorate supplier with Circuit Breaker and Rate Limiter
        Supplier<StreamResponse> downstreamSupplier = () ->
                downstreamClient.fetchStreamFromDownstream(contentId, clientId, simulateError, simulatedLatencyMs, randomFailureRate);

        Supplier<StreamResponse> rateLimitedSupplier = RateLimiter.decorateSupplier(clientLimiter, downstreamSupplier);
        Supplier<StreamResponse> protectedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, rateLimitedSupplier);

        long startTime = System.currentTimeMillis();
        try {
            StreamResponse response = protectedSupplier.get();
            response.setCircuitState(circuitBreaker.getState().name());
            return response;
        } catch (RequestNotPermitted ex) {
            log.warn("[RATE LIMIT EXCEEDED] Client '{}' exceeded maximum allowed request throughput: {}", clientId, ex.getMessage());
            return fallbackRateLimit(contentId, clientId, ex, System.currentTimeMillis() - startTime);
        } catch (CallNotPermittedException ex) {
            log.warn("[CIRCUIT OPEN] Call rejected fast for content '{}'. Circuit state: {}", contentId, circuitBreaker.getState());
            return fallbackCircuitBreaker(contentId, clientId, ex, System.currentTimeMillis() - startTime, true);
        } catch (Exception ex) {
            log.error("[DOWNSTREAM FAILURE] Failed to fetch stream for content '{}': {}", contentId, ex.getMessage());
            return fallbackCircuitBreaker(contentId, clientId, ex, System.currentTimeMillis() - startTime, false);
        }
    }

    /**
     * Automated fallback for Circuit Breaker tripping or downstream outages.
     * Degrades quality gracefully to local pre-cached CDN edge buffer (720p HD).
     */
    public StreamResponse fallbackCircuitBreaker(String contentId,
                                                 String clientId,
                                                 Throwable ex,
                                                 long durationMs,
                                                 boolean circuitOpen) {
        String cause = circuitOpen
                ? "Circuit Breaker OPEN (Fail-Fast Triggered)"
                : "Downstream Outage / Timeout (" + ex.getClass().getSimpleName() + ")";

        return StreamResponse.builder()
                .contentId(contentId)
                .clientId(clientId)
                .title("Stranger Things (Pre-Cached Edge Fallback)")
                .quality("720p HD (Edge Fallback Buffer)")
                .bitrate("2.5 Mbps (Optimized)")
                .status("FALLBACK")
                .message("Graceful Degradation Active: Downstream CDN unavailable [" + cause + "]. Serving cached edge stream without disruption.")
                .circuitState(circuitBreaker.getState().name())
                .executionDurationMs(durationMs)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    /**
     * Automated fallback for Sliding-Window Rate Limiter shedding.
     */
    public StreamResponse fallbackRateLimit(String contentId,
                                            String clientId,
                                            RequestNotPermitted ex,
                                            long durationMs) {
        return StreamResponse.builder()
                .contentId(contentId)
                .clientId(clientId)
                .title("Unknown / Throttled")
                .quality("NONE")
                .bitrate("0 Kbps")
                .status("THROTTLED")
                .message("HTTP 429 Too Many Requests: Sliding-window rate limit exceeded for client '" + clientId + "'. Surge protection engaged.")
                .circuitState(circuitBreaker.getState().name())
                .executionDurationMs(durationMs)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }
}

