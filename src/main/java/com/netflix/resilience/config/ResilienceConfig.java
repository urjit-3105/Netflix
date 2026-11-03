package com.netflix.resilience.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
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

    // In-memory buffer to track the last 50 circuit breaker & rate limiter events for real-time observability
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

        // Attach event consumers to record transitions and telemetry
        cb.getEventPublisher()
                .onStateTransition(event -> {
                    String msg = String.format("[%s] Circuit Breaker State Transition: %s -> %s",
                            Instant.now(), event.getStateTransition().getFromState(), event.getStateTransition().getToState());
                    log.warn(msg);
                    recordEvent(msg);
                })
                .onError(event -> {
                    String msg = String.format("[%s] Circuit Breaker Recorded Error: duration=%dms, error=%s",
                            Instant.now(), event.getElapsedDuration().toMillis(), event.getThrowable().getMessage());
                    log.debug(msg);
                    recordEvent(msg);
                })
                .onCallNotPermitted(event -> {
                    String msg = String.format("[%s] Circuit Breaker Call Blocked (OPEN State): Request failed fast",
                            Instant.now());
                    log.warn(msg);
                    recordEvent(msg);
                })
                .onSlowCallRateExceeded(event -> {
                    String msg = String.format("[%s] Circuit Breaker Slow Call Rate Exceeded: %.2f%%",
                            Instant.now(), event.getSlowCallRate());
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
                .limitForPeriod(10) // 10 requests allowed
                .limitRefreshPeriod(Duration.ofSeconds(1)) // per second sliding window
                .timeoutDuration(Duration.ofMillis(0)) // fail-fast with RequestNotPermittedException
                .build();
    }

    @Bean
    public RateLimiterRegistry rateLimiterRegistry(RateLimiterConfig rateLimiterConfig) {
        RateLimiterRegistry registry = RateLimiterRegistry.of(rateLimiterConfig);
        RateLimiter rl = registry.rateLimiter(DEFAULT_RATE_LIMITER_NAME);

        rl.getEventPublisher().onFailure(event -> {
            String msg = String.format("[%s] Rate Limiter Rejected Call: Client exceeded limit of 10 req/s",
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

