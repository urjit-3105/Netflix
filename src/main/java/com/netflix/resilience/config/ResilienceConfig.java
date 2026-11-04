package com.netflix.resilience.config;

import com.netflix.resilience.exception.DownstreamServiceException;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Configuration
public class ResilienceConfig {

    private static final Logger log = LoggerFactory.getLogger(ResilienceConfig.class);
    public static final String CIRCUIT_BREAKER_NAME = "streamingServiceCircuitBreaker";
    public static final String DEFAULT_RATE_LIMITER_NAME = "clientRateLimiter";
    public static final String BULKHEAD_NAME = "streamingBulkhead";
    public static final String RETRY_NAME = "streamingRetry";

    // In-memory buffer to track the last 50 resilience events for real-time observability
    private final ConcurrentLinkedDeque<String> resilienceEventHistory = new ConcurrentLinkedDeque<>();
    private static final int MAX_EVENT_HISTORY = 50;

    @Bean
    public CircuitBreakerConfig defaultCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50.0f)
                .slowCallRateThreshold(50.0f)
                .slowCallDurationThreshold(Duration.ofMillis(1500))
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(Throwable.class)
                .build();
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(CircuitBreakerConfig circuitBreakerConfig) {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        CircuitBreaker cb = registry.circuitBreaker(CIRCUIT_BREAKER_NAME);

        cb.getEventPublisher()
                .onStateTransition(event -> {
                    String msg = String.format("[%s] Circuit Breaker Transition: %s -> %s",
                            Instant.now(), event.getStateTransition().getFromState(), event.getStateTransition().getToState());
                    log.warn(msg);
                    recordEvent(msg);
                })
                .onError(event -> {
                    String msg = String.format("[%s] Circuit Breaker Error: %s",
                            Instant.now(), event.getThrowable().getMessage());
                    log.debug(msg);
                    recordEvent(msg);
                })
                .onCallNotPermitted(event -> {
                    String msg = String.format("[%s] Circuit Breaker Blocked: Call rejected fast (OPEN state)",
                            Instant.now());
                    log.warn(msg);
                    recordEvent(msg);
                });

        return registry;
    }

    @Bean
    public CircuitBreaker streamingServiceCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
        return circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
    }

    @Bean
    public RateLimiterConfig defaultRateLimiterConfig() {
        return RateLimiterConfig.custom()
                .limitForPeriod(10)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofMillis(0))
                .build();
    }

    @Bean
    public RateLimiterRegistry rateLimiterRegistry(RateLimiterConfig rateLimiterConfig) {
        RateLimiterRegistry registry = RateLimiterRegistry.of(rateLimiterConfig);
        RateLimiter rl = registry.rateLimiter(DEFAULT_RATE_LIMITER_NAME);

        rl.getEventPublisher().onFailure(event -> {
            String msg = String.format("[%s] Rate Limiter Throttled: Client burst exceeded throughput quota",
                    Instant.now());
            log.warn(msg);
            recordEvent(msg);
        });

        return registry;
    }

    @Bean
    public RateLimiter clientRateLimiter(RateLimiterRegistry rateLimiterRegistry) {
        return rateLimiterRegistry.rateLimiter(DEFAULT_RATE_LIMITER_NAME);
    }

    @Bean
    public BulkheadConfig defaultBulkheadConfig() {
        return BulkheadConfig.custom()
                .maxConcurrentCalls(10)
                .maxWaitDuration(Duration.ofMillis(10))
                .build();
    }

    @Bean
    public BulkheadRegistry bulkheadRegistry(BulkheadConfig bulkheadConfig) {
        BulkheadRegistry registry = BulkheadRegistry.of(bulkheadConfig);
        Bulkhead bh = registry.bulkhead(BULKHEAD_NAME);

        bh.getEventPublisher().onCallRejected(event -> {
            String msg = String.format("[%s] Bulkhead Saturated: Concurrent call rejected (Capacity=%d)",
                    Instant.now(), bh.getBulkheadConfig().getMaxConcurrentCalls());
            log.warn(msg);
            recordEvent(msg);
        });

        return registry;
    }

    @Bean
    public Bulkhead streamingBulkhead(BulkheadRegistry bulkheadRegistry) {
        return bulkheadRegistry.bulkhead(BULKHEAD_NAME);
    }

    @Bean
    public RetryConfig defaultRetryConfig() {
        return RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(100))
                .retryExceptions(DownstreamServiceException.class)
                .build();
    }

    @Bean
    public RetryRegistry retryRegistry(RetryConfig retryConfig) {
        RetryRegistry registry = RetryRegistry.of(retryConfig);
        Retry retry = registry.retry(RETRY_NAME);

        retry.getEventPublisher()
                .onRetry(event -> {
                    String msg = String.format("[%s] Retry Attempt #%d for '%s' (Waiting %dms)",
                            Instant.now(), event.getNumberOfRetryAttempts(), event.getName(), event.getWaitInterval().toMillis());
                    log.info(msg);
                    recordEvent(msg);
                })
                .onError(event -> {
                    String msg = String.format("[%s] Retry Exhausted: %d attempts failed: %s",
                            Instant.now(), event.getNumberOfRetryAttempts(), event.getLastThrowable().getMessage());
                    log.warn(msg);
                    recordEvent(msg);
                });

        return registry;
    }

    @Bean
    public Retry streamingRetry(RetryRegistry retryRegistry) {
        return retryRegistry.retry(RETRY_NAME);
    }

    public void recordEvent(String event) {
        resilienceEventHistory.addFirst(event);
        while (resilienceEventHistory.size() > MAX_EVENT_HISTORY) {
            resilienceEventHistory.removeLast();
        }
    }

    public List<String> getRecentResilienceEvents() {
        return resilienceEventHistory.stream().limit(25).collect(Collectors.toList());
    }
}
