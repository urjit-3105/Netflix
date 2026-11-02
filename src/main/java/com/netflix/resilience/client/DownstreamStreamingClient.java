package com.netflix.resilience.client;

import com.netflix.resilience.model.StreamResponse;

public interface DownstreamStreamingClient {

    /**
     * Remote call to downstream Netflix CDN transcode/playback service.
     *
     * @param contentId Content ID
     * @param clientId Client ID
     * @param simulateError If true, throws DownstreamServiceException
     * @param simulatedLatencyMs Injected latency in ms
     * @param randomFailureRatePercentage Probability (0-100) of random failure
     * @return StreamResponse
     */
    StreamResponse fetchStreamFromDownstream(String contentId,
                                            String clientId,
                                            boolean simulateError,
                                            long simulatedLatencyMs,
                                            int randomFailureRatePercentage);
}
