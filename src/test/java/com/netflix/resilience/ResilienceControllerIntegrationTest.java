package com.netflix.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class ResilienceControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CircuitBreaker streamingServiceCircuitBreaker;

    @BeforeEach
    void resetResilienceState() {
        streamingServiceCircuitBreaker.reset();
    }

    @Test
    @DisplayName("GET /api/v1/content/stream returns 200 and healthy 4K stream metadata")
    void testStreamContentSuccess() throws Exception {
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "movie-stranger-things")
                        .param("clientId", "sub-integration-test-1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("OK")))
                .andExpect(jsonPath("$.quality", containsString("4K")))
                .andExpect(jsonPath("$.circuitState", notNullValue()));
    }

    @Test
    @DisplayName("GET /api/v1/content/stream?fail=true triggers fallback metadata with 200 status")
    void testStreamContentDownstreamFailureTriggersFallback() throws Exception {
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "movie-downstream-fail")
                        .param("clientId", "sub-integration-test-2")
                        .param("fail", "true")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("FALLBACK")))
                .andExpect(jsonPath("$.quality", containsString("720p")))
                .andExpect(jsonPath("$.message", containsString("Downstream CDN unavailable")));
    }

    @Test
    @DisplayName("GET /api/v1/content/health-status returns circuit breaker metrics and rate limiter stats")
    void testGetHealthStatus() throws Exception {
        mockMvc.perform(get("/api/v1/content/health-status")
                        .param("clientId", "sub-integration-test-1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.serviceName", is("netflix-resilience-microservice")))
                .andExpect(jsonPath("$.circuitBreaker.name", notNullValue()))
                .andExpect(jsonPath("$.circuitBreaker.state", notNullValue()))
                .andExpect(jsonPath("$.rateLimiter.availablePermissions", notNullValue()));
    }

    @Test
    @DisplayName("POST /api/v1/content/simulate-load dispatches concurrent requests successfully")
    void testSimulateConcurrentLoad() throws Exception {
        mockMvc.perform(post("/api/v1/content/simulate-load")
                        .param("totalRequests", "10")
                        .param("concurrency", "4")
                        .param("failPercentage", "40")
                        .param("clientId", "integration-load-tester")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalRequestsDispatched", is(10)))
                .andExpect(jsonPath("$.finalCircuitState", notNullValue()));
    }
}
