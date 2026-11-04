package com.netflix.resilience;

import com.netflix.resilience.service.cache.CatalogCacheService;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class TieredCachingResilienceTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CatalogCacheService catalogCacheService;

    @Autowired
    private CircuitBreaker streamingServiceCircuitBreaker;

    @BeforeEach
    void setUp() {
        catalogCacheService.clear();
        streamingServiceCircuitBreaker.reset();
    }

    @Test
    @DisplayName("Initial successful streaming call populates both L1 Caffeine and Stale Cache")
    void testSuccessfulRequestPopulatesCache() throws Exception {
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "movie-squid-game-s2")
                        .param("clientId", "sub-cache-test-1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("OK")))
                .andExpect(jsonPath("$.quality", containsString("4K")));

        assertTrue(catalogCacheService.getStaleFallback("movie-squid-game-s2").isPresent());
    }

    @Test
    @DisplayName("Downstream outage on cached content seamlessly serves 4K manifest via Stale-While-Revalidate")
    void testStaleWhileRevalidateServesCached4K() throws Exception {
        // Step 1: Prime cache with initial 4K playback
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "movie-wednesday-s2")
                        .param("clientId", "sub-cache-test-2")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("OK")))
                .andExpect(jsonPath("$.quality", containsString("4K")));

        // Step 2: Induce downstream failure on the same content
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "movie-wednesday-s2")
                        .param("clientId", "sub-cache-test-2")
                        .param("fail", "true")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Cache-Degraded", "STALE_FALLBACK"))
                .andExpect(jsonPath("$.status", is("STALE_FALLBACK")))
                .andExpect(jsonPath("$.quality", containsString("4K UHD Dolby Vision")))
                .andExpect(jsonPath("$.message", containsString("Stale-While-Revalidate fallback")));
    }

    @Test
    @DisplayName("Cold cache request on downstream outage gracefully falls back to 720p edge buffer")
    void testColdCacheFallsBackTo720p() throws Exception {
        mockMvc.perform(get("/api/v1/content/stream")
                        .param("contentId", "movie-cold-content-unknown")
                        .param("clientId", "sub-cache-test-3")
                        .param("fail", "true")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Cache-Degraded", "COLD_FALLBACK"))
                .andExpect(jsonPath("$.status", is("FALLBACK")))
                .andExpect(jsonPath("$.quality", containsString("720p HD (Edge Fallback Buffer)")));
    }

    @Test
    @DisplayName("GET /dashboard serves the live visual telemetry web dashboard")
    void testDashboardEndpointServesUI() throws Exception {
        mockMvc.perform(get("/dashboard"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/dashboard.html"));
    }
}
