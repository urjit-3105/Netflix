package com.netflix.resilience.service.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.netflix.resilience.config.CacheConfig;
import com.netflix.resilience.model.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CatalogCacheService {

    private static final Logger log = LoggerFactory.getLogger(CatalogCacheService.class);

    private final Cache<Object, Object> l1CaffeineCache;
    // Long-lived Stale Store: Preserves the last known 4K Dolby Vision manifest indefinitely for degraded fallback
    private final Map<String, StreamResponse> staleStore = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public CatalogCacheService(CacheManager cacheManager) {
        org.springframework.cache.Cache springCache = cacheManager.getCache(CacheConfig.STREAM_MANIFEST_CACHE);
        if (springCache instanceof CaffeineCache caffeineCache) {
            this.l1CaffeineCache = caffeineCache.getNativeCache();
        } else {
            this.l1CaffeineCache = null;
        }
    }

    /**
     * Stores pristine 4K playback manifest in both L1 Caffeine cache and the long-lived Stale Store.
     */
    public void recordSuccessfulManifest(String contentId, StreamResponse response) {
        if (contentId == null || response == null) {
            return;
        }

        if (l1CaffeineCache != null) {
            l1CaffeineCache.put(contentId, response);
        }

        // Deep copy / snapshot into stale store
        StreamResponse staleSnapshot = StreamResponse.builder()
                .contentId(response.getContentId())
                .clientId(response.getClientId())
                .title(response.getTitle())
                .quality(response.getQuality())
                .bitrate(response.getBitrate())
                .status("STALE_FALLBACK")
                .cacheStatus("STALE_FALLBACK")
                .message("Downstream outage detected. Serving cached 4K UHD Dolby Vision manifest via Stale-While-Revalidate fallback.")
                .circuitState(response.getCircuitState())
                .executionDurationMs(response.getExecutionDurationMs())
                .timestamp(System.currentTimeMillis())
                .build();

        staleStore.put(contentId, staleSnapshot);
        log.info("[TIERED CACHE] Cached 4K manifest for contentId='{}' in L1 and Stale Fallback Store", contentId);
    }

    /**
     * Retrieves cached 4K manifest from the Stale Store when downstream or circuit breaker fails.
     */
    public Optional<StreamResponse> getStaleFallback(String contentId) {
        if (contentId == null) {
            return Optional.empty();
        }
        StreamResponse cached = staleStore.get(contentId);
        if (cached != null) {
            log.warn("[TIERED CACHE] Stale-While-Revalidate HIT for contentId='{}'. Serving cached 4K manifest.", contentId);
            return Optional.of(StreamResponse.builder()
                    .contentId(cached.getContentId())
                    .clientId(cached.getClientId())
                    .title(cached.getTitle())
                    .quality(cached.getQuality())
                    .bitrate(cached.getBitrate())
                    .status("STALE_FALLBACK")
                    .cacheStatus("STALE_FALLBACK")
                    .message("Downstream outage detected. Serving cached 4K UHD Dolby Vision manifest via Stale-While-Revalidate fallback.")
                    .circuitState(cached.getCircuitState())
                    .executionDurationMs(cached.getExecutionDurationMs())
                    .timestamp(System.currentTimeMillis())
                    .build());
        }
        return Optional.empty();
    }

    public int getStaleStoreSize() {
        return staleStore.size();
    }

    public void clear() {
        if (l1CaffeineCache != null) {
            l1CaffeineCache.invalidateAll();
        }
        staleStore.clear();
        log.info("[TIERED CACHE] All L1 and Stale Cache stores invalidated.");
    }
}
