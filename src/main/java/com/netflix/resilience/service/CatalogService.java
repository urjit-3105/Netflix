package com.netflix.resilience.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CatalogService {

    private static final Logger log = LoggerFactory.getLogger(CatalogService.class);

    private final Map<String, String> cachedCatalog = new ConcurrentHashMap<>();

    public CatalogService() {
        cachedCatalog.put("movie-101", "Stranger Things (Cached Fallback)");
        cachedCatalog.put("movie-102", "The Crown (Cached Fallback)");
        cachedCatalog.put("movie-103", "Squid Game (Cached Fallback)");
    }

    /**
     * Simulates fetching video metadata from an external distributed service.
     * Protected by Resilience4j Circuit Breaker and Rate Limiter.
     *
     * @param videoId ID of the video/movie
     * @param simulateFailure If true, triggers downstream exception to trip circuit
     * @param delayMs Simulated latency in milliseconds
     * @return Video metadata
     */
    @CircuitBreaker(name = "catalogService", fallbackMethod = "catalogFallback")
    @RateLimiter(name = "streamLimiter", fallbackMethod = "rateLimitFallback")
    public String getVideoMetadata(String videoId, boolean simulateFailure, long delayMs) {
        log.info("Processing metadata request for video: {}, simulateFailure={}, delayMs={}", videoId, simulateFailure, delayMs);

        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted during execution", e);
            }
        }

        if (simulateFailure) {
            log.error("Downstream catalog dependency service failure triggered for video: {}", videoId);
            throw new RuntimeException("Downstream Catalog Service is unavailable or timed out!");
        }

        return String.format("Live Video Metadata: [ID: %s, Title: Streaming Hit, Status: OK, Bitrate: 4K UltraHD]", videoId);
    }

    /**
     * Fallback for Circuit Breaker when service fails or circuit is open.
     */
    public String catalogFallback(String videoId, boolean simulateFailure, long delayMs, Throwable ex) {
        log.warn("Circuit Breaker fallback invoked for video: {}. Reason: {}", videoId, ex.getMessage());
        String fallbackTitle = cachedCatalog.getOrDefault(videoId, "Popular Netflix Content (Standard Definition)");
        return String.format("[CIRCUIT BREAKER FALLBACK] Serving cached fallback metadata for '%s'. Downstream error: %s",
                fallbackTitle, ex.getClass().getSimpleName() + " - " + ex.getMessage());
    }

    /**
     * Fallback for Rate Limiter when request limit exceeded.
     */
    public String rateLimitFallback(String videoId, boolean simulateFailure, long delayMs, io.github.resilience4j.ratelimiter.RequestNotPermitted ex) {
        log.warn("Rate Limiter triggered for video: {}. Too many concurrent requests.", videoId);
        return String.format("[RATE LIMIT FALLBACK] Request rejected by sliding-window rate limiter for '%s'. Please retry after cooling period.", videoId);
    }
}

