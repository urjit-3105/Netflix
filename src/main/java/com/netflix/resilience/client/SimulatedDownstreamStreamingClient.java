package com.netflix.resilience.client;

import com.netflix.resilience.exception.DownstreamServiceException;
import com.netflix.resilience.model.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class SimulatedDownstreamStreamingClient implements DownstreamStreamingClient {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDownstreamStreamingClient.class);
    private final Random random = new Random();

    @Override
    public StreamResponse fetchStreamFromDownstream(String contentId,
                                                   String clientId,
                                                   boolean simulateError,
                                                   long simulatedLatencyMs,
                                                   int randomFailureRatePercentage) {
        long startTime = System.currentTimeMillis();
        log.info("[DOWNSTREAM CLIENT] Requesting stream chunk for content='{}', client='{}', simulateError={}, latency={}ms",
                contentId, clientId, simulateError, simulatedLatencyMs);

        // Inject simulated latency (for testing slow call thresholds)
        if (simulatedLatencyMs > 0) {
            try {
                Thread.sleep(simulatedLatencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DownstreamServiceException("Downstream call interrupted during latency simulation", e);
            }
        }

        // Inject explicit or probabilistic failure
        boolean shouldFail = simulateError || (randomFailureRatePercentage > 0 && random.nextInt(100) < randomFailureRatePercentage);

        if (shouldFail) {
            log.error("[DOWNSTREAM CLIENT] Simulating 503 Service Unavailable / Connection Timeout for content='{}'", contentId);
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
                .message("High-speed direct streaming active from primary CDN edge node")
                .circuitState("UNKNOWN")
                .executionDurationMs(duration)
                .timestamp(System.currentTimeMillis())
                .build();
    }
}

