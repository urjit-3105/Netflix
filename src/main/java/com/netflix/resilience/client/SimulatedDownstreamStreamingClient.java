package com.netflix.resilience.client;

import com.netflix.resilience.exception.DownstreamServiceException;
import com.netflix.resilience.model.StreamResponse;
import com.netflix.resilience.service.chaos.ChaosService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class SimulatedDownstreamStreamingClient implements DownstreamStreamingClient {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDownstreamStreamingClient.class);
    private final Random random = new Random();
    private final ChaosService chaosService;

    public SimulatedDownstreamStreamingClient(ChaosService chaosService) {
        this.chaosService = chaosService;
    }

    @Override
    public StreamResponse fetchStreamFromDownstream(String contentId,
                                                   String clientId,
                                                   boolean simulateError,
                                                   long simulatedLatencyMs,
                                                   int randomFailureRatePercentage) {
        long startTime = System.currentTimeMillis();

        // Check dynamic Chaos Monkey latency
        long dynamicChaosLatency = (chaosService != null) ? chaosService.computeSimulatedLatency() : 0;
        long totalLatency = simulatedLatencyMs + dynamicChaosLatency;

        log.info("[DOWNSTREAM CLIENT] Requesting stream chunk for content='{}', client='{}', simulateError={}, latency={}ms (chaos latency: {}ms)",
                contentId, clientId, simulateError, totalLatency, dynamicChaosLatency);

        // Inject simulated latency (for testing slow call thresholds)
        if (totalLatency > 0) {
            try {
                Thread.sleep(totalLatency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DownstreamServiceException("Downstream call interrupted during latency simulation", e);
            }
        }

        // Check dynamic Chaos Monkey failure or explicit failure
        boolean chaosFailure = (chaosService != null) && chaosService.shouldInjectFailure();
        boolean argFailure = simulateError || (randomFailureRatePercentage > 0 && random.nextInt(100) < randomFailureRatePercentage);
        boolean shouldFail = chaosFailure || argFailure;

        if (shouldFail) {
            log.error("[DOWNSTREAM CLIENT] Simulating 503 Service Unavailable / Connection Timeout for content='{}' (chaos: {}, explicit: {})",
                    contentId, chaosFailure, argFailure);
            throw new DownstreamServiceException("Downstream CDN Transcoder Service unreachable or returned HTTP 503 Service Unavailable");
        }

        long duration = System.currentTimeMillis() - startTime;
        return StreamResponse.builder()
                .contentId(contentId)
                .clientId(clientId)
                .title("Stranger Things Season 5 (Ultra HD Master)")
                .quality("4K UHD Dolby Vision")
                .bitrate("18.5 Mbps")
                .status("OK")
                .cacheStatus("ORIGIN_CDN")
                .message("High-speed direct streaming active from primary CDN edge node")
                .circuitState("UNKNOWN")
                .executionDurationMs(duration)
                .timestamp(System.currentTimeMillis())
                .build();
    }
}
