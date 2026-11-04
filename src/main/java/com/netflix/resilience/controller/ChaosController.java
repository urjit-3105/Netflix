package com.netflix.resilience.controller;

import com.netflix.resilience.model.ChaosConfig;
import com.netflix.resilience.service.chaos.ChaosService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/chaos")
public class ChaosController {

    private final ChaosService chaosService;

    public ChaosController(ChaosService chaosService) {
        this.chaosService = chaosService;
    }

    @GetMapping("/status")
    public ResponseEntity<ChaosConfig> getChaosStatus() {
        return ResponseEntity.ok(chaosService.getStatus());
    }

    @PostMapping("/configure")
    public ResponseEntity<ChaosConfig> configureChaos(
            @RequestParam(name = "enabled", defaultValue = "true") boolean enabled,
            @RequestParam(name = "failureRate", defaultValue = "50") int failureRate,
            @RequestParam(name = "latencyMs", defaultValue = "0") long latencyMs,
            @RequestParam(name = "jitterMs", defaultValue = "0") long jitterMs,
            @RequestParam(name = "partition", defaultValue = "false") boolean partition) {

        chaosService.configure(enabled, failureRate, latencyMs, jitterMs, partition);
        return ResponseEntity.ok(chaosService.getStatus());
    }

    @PostMapping("/partition")
    public ResponseEntity<Map<String, Object>> togglePartition(
            @RequestParam(name = "active", defaultValue = "true") boolean active) {

        chaosService.setDownstreamPartition(active);
        if (active) {
            chaosService.setEnabled(true);
        }
        return ResponseEntity.ok(Map.of(
                "message", active ? "Downstream network partition injected (100% outage)" : "Downstream network partition resolved",
                "partitionActive", active,
                "chaosStatus", chaosService.getStatus()
        ));
    }

    @PostMapping("/reset")
    public ResponseEntity<Map<String, Object>> resetChaos() {
        chaosService.reset();
        return ResponseEntity.ok(Map.of(
                "message", "Chaos Monkey reset successfully. All synthetic failures disabled.",
                "chaosStatus", chaosService.getStatus()
        ));
    }
}
