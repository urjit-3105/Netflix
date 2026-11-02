package com.netflix.resilience.model;

public class StreamResponse {
    private String contentId;
    private String clientId;
    private String title;
    private String quality;
    private String bitrate;
    private String status;
    private String message;
    private String circuitState;
    private long executionDurationMs;
    private long timestamp;

    public StreamResponse() {}

    public StreamResponse(String contentId, String clientId, String title, String quality,
                          String bitrate, String status, String message, String circuitState,
                          long executionDurationMs, long timestamp) {
        this.contentId = contentId;
        this.clientId = clientId;
        this.title = title;
        this.quality = quality;
        this.bitrate = bitrate;
        this.status = status;
        this.message = message;
        this.circuitState = circuitState;
        this.executionDurationMs = executionDurationMs;
        this.timestamp = timestamp;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String contentId;
        private String clientId;
        private String title;
        private String quality;
        private String bitrate;
        private String status;
        private String message;
        private String circuitState;
        private long executionDurationMs;
        private long timestamp;

        public Builder contentId(String contentId) { this.contentId = contentId; return this; }
        public Builder clientId(String clientId) { this.clientId = clientId; return this; }
        public Builder title(String title) { this.title = title; return this; }
        public Builder quality(String quality) { this.quality = quality; return this; }
        public Builder bitrate(String bitrate) { this.bitrate = bitrate; return this; }
        public Builder status(String status) { this.status = status; return this; }
        public Builder message(String message) { this.message = message; return this; }
        public Builder circuitState(String circuitState) { this.circuitState = circuitState; return this; }
        public Builder executionDurationMs(long executionDurationMs) { this.executionDurationMs = executionDurationMs; return this; }
        public Builder timestamp(long timestamp) { this.timestamp = timestamp; return this; }

        public StreamResponse build() {
            return new StreamResponse(contentId, clientId, title, quality, bitrate, status, message, circuitState, executionDurationMs, timestamp);
        }
    }

    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }
    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getQuality() { return quality; }
    public void setQuality(String quality) { this.quality = quality; }
    public String getBitrate() { return bitrate; }
    public void setBitrate(String bitrate) { this.bitrate = bitrate; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getCircuitState() { return circuitState; }
    public void setCircuitState(String circuitState) { this.circuitState = circuitState; }
    public long getExecutionDurationMs() { return executionDurationMs; }
    public void setExecutionDurationMs(long executionDurationMs) { this.executionDurationMs = executionDurationMs; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
