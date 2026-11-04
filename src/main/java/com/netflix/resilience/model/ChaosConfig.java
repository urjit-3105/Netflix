package com.netflix.resilience.model;

public class ChaosConfig {
    private boolean enabled;
    private int failureRatePercentage;
    private long latencyMs;
    private long latencyJitterMs;
    private boolean downstreamPartition;
    private long totalChaosInjections;
    private long lastInjectedTimestamp;

    public ChaosConfig() {
        this.enabled = false;
        this.failureRatePercentage = 0;
        this.latencyMs = 0;
        this.latencyJitterMs = 0;
        this.downstreamPartition = false;
        this.totalChaosInjections = 0;
        this.lastInjectedTimestamp = 0;
    }

    public ChaosConfig(boolean enabled, int failureRatePercentage, long latencyMs,
                       long latencyJitterMs, boolean downstreamPartition,
                       long totalChaosInjections, long lastInjectedTimestamp) {
        this.enabled = enabled;
        this.failureRatePercentage = failureRatePercentage;
        this.latencyMs = latencyMs;
        this.latencyJitterMs = latencyJitterMs;
        this.downstreamPartition = downstreamPartition;
        this.totalChaosInjections = totalChaosInjections;
        this.lastInjectedTimestamp = lastInjectedTimestamp;
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getFailureRatePercentage() { return failureRatePercentage; }
    public void setFailureRatePercentage(int failureRatePercentage) { this.failureRatePercentage = failureRatePercentage; }
    public long getLatencyMs() { return latencyMs; }
    public void setLatencyMs(long latencyMs) { this.latencyMs = latencyMs; }
    public long getLatencyJitterMs() { return latencyJitterMs; }
    public void setLatencyJitterMs(long latencyJitterMs) { this.latencyJitterMs = latencyJitterMs; }
    public boolean isDownstreamPartition() { return downstreamPartition; }
    public void setDownstreamPartition(boolean downstreamPartition) { this.downstreamPartition = downstreamPartition; }
    public long getTotalChaosInjections() { return totalChaosInjections; }
    public void setTotalChaosInjections(long totalChaosInjections) { this.totalChaosInjections = totalChaosInjections; }
    public long getLastInjectedTimestamp() { return lastInjectedTimestamp; }
    public void setLastInjectedTimestamp(long lastInjectedTimestamp) { this.lastInjectedTimestamp = lastInjectedTimestamp; }
}
