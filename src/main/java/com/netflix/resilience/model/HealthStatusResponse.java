package com.netflix.resilience.model;

import java.util.List;
import java.util.Map;

public class HealthStatusResponse {
    private String serviceName;
    private String overallHealth;
    private Map<String, Object> circuitBreaker;
    private Map<String, Object> rateLimiter;
    private List<String> recentResilienceEvents;
    private long timestamp;

    public HealthStatusResponse() {}

    public HealthStatusResponse(String serviceName, String overallHealth, Map<String, Object> circuitBreaker,
                                Map<String, Object> rateLimiter, List<String> recentResilienceEvents, long timestamp) {
        this.serviceName = serviceName;
        this.overallHealth = overallHealth;
        this.circuitBreaker = circuitBreaker;
        this.rateLimiter = rateLimiter;
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
        private List<String> recentResilienceEvents;
        private long timestamp;

        public Builder serviceName(String serviceName) { this.serviceName = serviceName; return this; }
        public Builder overallHealth(String overallHealth) { this.overallHealth = overallHealth; return this; }
        public Builder circuitBreaker(Map<String, Object> circuitBreaker) { this.circuitBreaker = circuitBreaker; return this; }
        public Builder rateLimiter(Map<String, Object> rateLimiter) { this.rateLimiter = rateLimiter; return this; }
        public Builder recentResilienceEvents(List<String> recentResilienceEvents) { this.recentResilienceEvents = recentResilienceEvents; return this; }
        public Builder timestamp(long timestamp) { this.timestamp = timestamp; return this; }

        public HealthStatusResponse build() {
            return new HealthStatusResponse(serviceName, overallHealth, circuitBreaker, rateLimiter, recentResilienceEvents, timestamp);
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
    public List<String> getRecentResilienceEvents() { return recentResilienceEvents; }
    public void setRecentResilienceEvents(List<String> recentResilienceEvents) { this.recentResilienceEvents = recentResilienceEvents; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
