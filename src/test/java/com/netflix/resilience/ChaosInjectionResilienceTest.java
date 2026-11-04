package com.netflix.resilience;

import com.netflix.resilience.service.chaos.ChaosService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.AfterEach;
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
class ChaosInjectionResilienceTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ChaosService chaosService;

    @Autowired
    private CircuitBreaker streamingServiceCircuitBreaker;

    @BeforeEach
    void setUp() {
        chaosService.reset();
        streamingServiceCircuitBreaker.reset();
    }

    @AfterEach
    void tearDown() {
        chaosService.reset();
        streamingServiceCircuitBreaker.reset();
    }

    @Test
    @DisplayName("GET /api/v1/chaos/status returns nominal default configuration")
    void testNominalChaosStatus() throws Exception {
        mockMvc.perform(get("/api/v1/chaos/status")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.enabled", is(false)))
                .andExpect(jsonPath("$.failureRatePercentage", is(0)))
                .andExpect(jsonPath("$.downstreamPartition", is(false)));
    }

    @Test
    @DisplayName("POST /api/v1/chaos/configure injects 100% failure rate and triggers fallback at runtime")
    void testRuntimeChaosFailureInjection() throws Exception {
        // Step 1: Configure 100% simulated failure via Chaos Monkey API
        mockMvc.perform(post("/api/v1/chaos/configure")
                        .param("enabled", "true")
                        .param("failureRate", "100")
                        .param("latencyMs", "10")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.enabled", is(true)))
                .andExpect(jsonPath("$.failureRatePercentage", is(100)));

        // Step 2: Stream content should now fail and trigger fallback even without pass-through query params
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "chaos-movie-test")
                        .param("clientId", "chaos-client-1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("FALLBACK")))
                .andExpect(jsonPath("$.message", containsString("Downstream CDN unavailable")));

        // Step 3: Reset chaos rules and circuit
        mockMvc.perform(post("/api/v1/chaos/reset")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.chaosStatus.enabled", is(false)));

        streamingServiceCircuitBreaker.reset();

        // Step 4: Normal operation immediately restored
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "chaos-movie-test")
                        .param("clientId", "chaos-client-1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("OK")))
                .andExpect(jsonPath("$.quality", containsString("4K")));
    }

    @Test
    @DisplayName("POST /api/v1/chaos/partition triggers immediate network partition outage")
    void testNetworkPartitionOutage() throws Exception {
        mockMvc.perform(post("/api/v1/chaos/partition")
                        .param("active", "true")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partitionActive", is(true)));

        // Call streaming endpoint
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "partition-movie")
                        .param("clientId", "chaos-client-2")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("FALLBACK")));

        // Disable partition
        mockMvc.perform(post("/api/v1/chaos/partition")
                        .param("active", "false")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partitionActive", is(false)));
    }
}
