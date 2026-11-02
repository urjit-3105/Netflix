package com.netflix.resilience;

import com.netflix.resilience.service.CatalogService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class NetflixResilienceApplicationTests {

    @Autowired
    private CatalogService catalogService;

    @Test
    void contextLoads() {
        assertNotNull(catalogService);
    }

    @Test
    void testCircuitBreakerFallbackOnFailure() {
        String result = catalogService.getVideoMetadata("test-vid-1", true, 0);
        assertNotNull(result);
        assertTrue(result.contains("[CIRCUIT BREAKER FALLBACK]"));
    }
}
