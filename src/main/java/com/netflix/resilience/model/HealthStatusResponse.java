package com.netflix.resilience.model;

import java.util.List;
import java.util.Map;

public class HealthStatusResponse {
    private String serviceName;
    private String overallHealth;
    private Map<String, Object> circuitBreaker;
    private Map<String, Object> rateLimiter;
    private Map<String, Object> bulkhead;
    private Map<String, Object> retry;
    private Map<String, Object> cache;
    private List<String> recentResilienceEvents;
    private long timestamp;

    public HealthStatusResponse() {}

    public HealthStatusResponse(String serviceName, String overallHealth,
                                Map<String, Object> circuitBreaker,
                                Map<String, Object> rateLimiter,
                                Map<String, Object> bulkhead,
                                Map<String, Object> retry,
                                Map<String, Object> cache,
                                List<String> recentResilienceEvents,
                                long timestamp) {
        this.serviceName = serviceName;
        this.overallHealth = overallHealth;
        this.circuitBreaker = circuitBreaker;
        this.rateLimiter = rateLimiter;
        this.bulkhead = bulkhead;
        this.retry = retry;
        this.cache = cache;
        this.recentResilienceEvents = recentResilienceEvents;
        this.timestamp = timestamp;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String serviceName;
        private String overallHealth;
        private Map<String, Object> circuitBreaker;
        private Map<String, Object> rateLimiter;
        private Map<String, Object> bulkhead;
        private Map<String, Object> retry;
        private Map<String, Object> cache;
        private List<String> recentResilienceEvents;
        private long timestamp;

        public Builder serviceName(String serviceName) { this.serviceName = serviceName; return this; }
        public Builder overallHealth(String overallHealth) { this.overallHealth = overallHealth; return this; }
        public Builder circuitBreaker(Map<String, Object> circuitBreaker) { this.circuitBreaker = circuitBreaker; return this; }
        public Builder rateLimiter(Map<String, Object> rateLimiter) { this.rateLimiter = rateLimiter; return this; }
        public Builder bulkhead(Map<String, Object> bulkhead) { this.bulkhead = bulkhead; return this; }
        public Builder retry(Map<String, Object> retry) { this.retry = retry; return this; }
        public Builder cache(Map<String, Object> cache) { this.cache = cache; return this; }
        public Builder recentResilienceEvents(List<String> recentResilienceEvents) { this.recentResilienceEvents = recentResilienceEvents; return this; }
        public Builder timestamp(long timestamp) { this.timestamp = timestamp; return this; }

        public HealthStatusResponse build() {
            return new HealthStatusResponse(serviceName, overallHealth, circuitBreaker, rateLimiter,
                    bulkhead, retry, cache, recentResilienceEvents, timestamp);
        }
    }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }
    public String getOverallHealth() { return overallHealth; }
    public void setOverallHealth(String overallHealth) { this.overallHealth = overallHealth; }
    public Map<String, Object> getCircuitBreaker() { return circuitBreaker; }
    public void setCircuitBreaker(Map<String, Object> circuitBreaker) { this.circuitBreaker = circuitBreaker; }
    public Map<String, Object> getRateLimiter() { return rateLimiter; }
    public void setRateLimiter(Map<String, Object> rateLimiter) { this.rateLimiter = rateLimiter; }
    public Map<String, Object> getBulkhead() { return bulkhead; }
    public void setBulkhead(Map<String, Object> bulkhead) { this.bulkhead = bulkhead; }
    public Map<String, Object> getRetry() { return retry; }
    public void setRetry(Map<String, Object> retry) { this.retry = retry; }
    public Map<String, Object> getCache() { return cache; }
    public void setCache(Map<String, Object> cache) { this.cache = cache; }
    public List<String> getRecentResilienceEvents() { return recentResilienceEvents; }
    public void setRecentResilienceEvents(List<String> recentResilienceEvents) { this.recentResilienceEvents = recentResilienceEvents; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
