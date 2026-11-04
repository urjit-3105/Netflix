package com.netflix.resilience.service;

import com.netflix.resilience.client.DownstreamStreamingClient;
import com.netflix.resilience.config.ResilienceConfig;
import com.netflix.resilience.model.StreamResponse;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.retry.Retry;
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
    private final Bulkhead bulkhead;
    private final Retry retry;
    private final ResilienceConfig resilienceConfig;

    public StreamingService(DownstreamStreamingClient downstreamClient,
                            CircuitBreaker streamingServiceCircuitBreaker,
                            RateLimiterRegistry rateLimiterRegistry,
                            Bulkhead streamingBulkhead,
                            Retry streamingRetry,
                            ResilienceConfig resilienceConfig) {
        this.downstreamClient = downstreamClient;
        this.circuitBreaker = streamingServiceCircuitBreaker;
        this.rateLimiterRegistry = rateLimiterRegistry;
        this.bulkhead = streamingBulkhead;
        this.retry = streamingRetry;
        this.resilienceConfig = resilienceConfig;
    }

    /**
     * Executes streaming content retrieval protected by the full Resilience4j stack:
     * RateLimiter -> Bulkhead -> CircuitBreaker -> Retry -> Downstream Client
     *
     * @param contentId Content ID (e.g. movie/series identifier)
     * @param clientId Client / Subscriber identifier for rate-limiting
     * @param simulateError Flag to force remote failure
     * @param simulatedLatencyMs Simulated remote latency in milliseconds
     * @param randomFailureRate Percentage chance (0-100) of simulated random remote failure
     * @return StreamResponse with playback manifest, bitrate, quality, and resilience status
     */
    public StreamResponse streamContent(String contentId,
                                        String clientId,
                                        boolean simulateError,
                                        long simulatedLatencyMs,
                                        int randomFailureRate) {

        String rateLimiterKey = (clientId != null && !clientId.isBlank())
                ? "rateLimiter-" + clientId.trim()
                : ResilienceConfig.DEFAULT_RATE_LIMITER_NAME;

        RateLimiter clientLimiter = rateLimiterRegistry.rateLimiter(rateLimiterKey);

        // Core downstream call supplier
        Supplier<StreamResponse> downstreamSupplier = () ->
                downstreamClient.fetchStreamFromDownstream(contentId, clientId, simulateError, simulatedLatencyMs, randomFailureRate);

        // Layered resilience decorators:
        // 1. Retry: Retries transient exceptions up to 3 times with exponential backoff & jitter
        Supplier<StreamResponse> retrySupplier = Retry.decorateSupplier(retry, downstreamSupplier);
        // 2. Circuit Breaker: Tracks failure rates and fails-fast when threshold exceeded
        Supplier<StreamResponse> circuitProtectedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, retrySupplier);
        // 3. Bulkhead: Caps concurrent in-flight calls to isolate thread pools
        Supplier<StreamResponse> bulkheadSupplier = Bulkhead.decorateSupplier(bulkhead, circuitProtectedSupplier);
        // 4. Rate Limiter: Enforces per-client sliding window throughput
        Supplier<StreamResponse> fullyProtectedSupplier = RateLimiter.decorateSupplier(clientLimiter, bulkheadSupplier);

        long startTime = System.currentTimeMillis();
        try {
            StreamResponse response = fullyProtectedSupplier.get();
            response.setCircuitState(circuitBreaker.getState().name());
            return response;
        } catch (RequestNotPermitted ex) {
            log.warn("[RATE LIMIT EXCEEDED] Client '{}' exceeded maximum allowed request throughput: {}", clientId, ex.getMessage());
            return fallbackRateLimit(contentId, clientId, ex, System.currentTimeMillis() - startTime);
        } catch (BulkheadFullException ex) {
            log.warn("[BULKHEAD SATURATED] Concurrency limit exceeded on bulkhead '{}' for content '{}': {}",
                    bulkhead.getName(), contentId, ex.getMessage());
            return fallbackBulkhead(contentId, clientId, ex, System.currentTimeMillis() - startTime);
        } catch (CallNotPermittedException ex) {
            log.warn("[CIRCUIT OPEN] Call rejected fast for content '{}'. Circuit state: {}", contentId, circuitBreaker.getState());
            return fallbackCircuitBreaker(contentId, clientId, ex, System.currentTimeMillis() - startTime, true);
        } catch (Exception ex) {
            log.error("[DOWNSTREAM FAILURE] Failed to fetch stream for content '{}' after retries: {}", contentId, ex.getMessage());
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
                : "Downstream Outage (" + ex.getClass().getSimpleName() + " after retries)";

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

    /**
     * Automated fallback for Bulkhead concurrency saturation.
     */
    public StreamResponse fallbackBulkhead(String contentId,
                                           String clientId,
                                           BulkheadFullException ex,
                                           long durationMs) {
        return StreamResponse.builder()
                .contentId(contentId)
                .clientId(clientId)
                .title("Unknown / Bulkhead Saturated")
                .quality("NONE")
                .bitrate("0 Kbps")
                .status("BULKHEAD_FULL")
                .message("HTTP 503 Service Unavailable: Bulkhead concurrency capacity saturated for content '" + contentId + "'. System isolated against cascading load.")
                .circuitState(circuitBreaker.getState().name())
                .executionDurationMs(durationMs)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public Bulkhead getBulkhead() {
        return bulkhead;
    }

    public Retry getRetry() {
        return retry;
    }
}
