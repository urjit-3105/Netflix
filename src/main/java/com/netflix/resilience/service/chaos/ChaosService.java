package com.netflix.resilience.service.chaos;

import com.netflix.resilience.model.ChaosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ChaosService {

    private static final Logger log = LoggerFactory.getLogger(ChaosService.class);

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicInteger failureRatePercentage = new AtomicInteger(0);
    private final AtomicLong latencyMs = new AtomicLong(0);
    private final AtomicLong latencyJitterMs = new AtomicLong(0);
    private final AtomicBoolean downstreamPartition = new AtomicBoolean(false);
    private final AtomicLong totalChaosInjections = new AtomicLong(0);
    private final AtomicLong lastInjectedTimestamp = new AtomicLong(0);

    /**
     * Inspects active chaos rules and determines if the current call should fail.
     */
    public boolean shouldInjectFailure() {
        if (!enabled.get()) {
            return false;
        }

        if (downstreamPartition.get()) {
            recordInjection();
            log.warn("[CHAOS MONKEY] Injected complete DOWNSTREAM PARTITION outage!");
            return true;
        }

        int rate = failureRatePercentage.get();
        if (rate > 0 && ThreadLocalRandom.current().nextInt(100) < rate) {
            recordInjection();
            log.warn("[CHAOS MONKEY] Injected probabilistic HTTP 503 error (rate: {}%)", rate);
            return true;
        }

        return false;
    }

    /**
     * Calculates simulated latency with jitter based on current chaos parameters.
     */
    public long computeSimulatedLatency() {
        if (!enabled.get()) {
            return 0;
        }

        long base = latencyMs.get();
        long jitter = latencyJitterMs.get();
        if (base <= 0 && jitter <= 0) {
            return 0;
        }

        long addedJitter = jitter > 0 ? ThreadLocalRandom.current().nextLong(jitter + 1) : 0;
        long totalDelay = base + addedJitter;
        if (totalDelay > 0) {
            recordInjection();
            log.info("[CHAOS MONKEY] Injected artificial latency delay: {}ms (base: {}ms, jitter: {}ms)",
                    totalDelay, base, addedJitter);
        }
        return totalDelay;
    }

    private void recordInjection() {
        totalChaosInjections.incrementAndGet();
        lastInjectedTimestamp.set(System.currentTimeMillis());
    }

    /**
     * Dynamically update chaos configuration.
     */
    public void configure(boolean enabled, int failureRate, long latency, long jitter, boolean partition) {
        this.enabled.set(enabled);
        this.failureRatePercentage.set(Math.max(0, Math.min(100, failureRate)));
        this.latencyMs.set(Math.max(0, latency));
        this.latencyJitterMs.set(Math.max(0, jitter));
        this.downstreamPartition.set(partition);

        log.info("[CHAOS MONKEY] Updated rules -> enabled={}, failureRate={}% latency={}ms, jitter={}ms, partition={}",
                enabled, failureRatePercentage.get(), latencyMs.get(), latencyJitterMs.get(), partition);
    }

    /**
     * Resets all chaos settings to pristine conditions.
     */
    public void reset() {
        this.enabled.set(false);
        this.failureRatePercentage.set(0);
        this.latencyMs.set(0);
        this.latencyJitterMs.set(0);
        this.downstreamPartition.set(false);
        log.info("[CHAOS MONKEY] Reset all chaos rules. Normal operations restored.");
    }

    public ChaosConfig getStatus() {
        return new ChaosConfig(
                enabled.get(),
                failureRatePercentage.get(),
                latencyMs.get(),
                latencyJitterMs.get(),
                downstreamPartition.get(),
                totalChaosInjections.get(),
                lastInjectedTimestamp.get()
        );
    }

    public boolean isEnabled() { return enabled.get(); }
    public void setEnabled(boolean value) { enabled.set(value); }
    public void setDownstreamPartition(boolean value) { downstreamPartition.set(value); }
    public boolean isDownstreamPartition() { return downstreamPartition.get(); }
    public long getTotalChaosInjections() { return totalChaosInjections.get(); }
}
