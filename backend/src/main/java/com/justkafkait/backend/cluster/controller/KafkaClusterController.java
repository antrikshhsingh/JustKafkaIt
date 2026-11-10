
package com.justkafkait.backend.cluster.controller;

import com.justkafkait.backend.cluster.dto.CreateKafkaClusterRequest;
import com.justkafkait.backend.cluster.dto.KafkaClusterMetadataResponse;
import com.justkafkait.backend.cluster.dto.KafkaClusterResponse;
import com.justkafkait.backend.cluster.service.KafkaClusterConnectionService;
import com.justkafkait.backend.cluster.service.KafkaClusterService;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/clusters")
public class KafkaClusterController {

    private final KafkaClusterService clusterService;

    private final KafkaClusterConnectionService connectionService;

    public KafkaClusterController(KafkaClusterService clusterService, KafkaClusterConnectionService connectionService) {
        this.clusterService = clusterService;
        this.connectionService = connectionService;
    }

    @PostMapping
    public ResponseEntity<KafkaClusterResponse> createCluster(
            @Valid @RequestBody CreateKafkaClusterRequest request) {

        KafkaClusterResponse response = clusterService.createCluster(request);

        URI location = URI.create("/api/clusters/" + response.id());

        return ResponseEntity.created(location).body(response);
    }

    @GetMapping
    public ResponseEntity<List<KafkaClusterResponse>> getAllClusters() {

        return ResponseEntity.ok(clusterService.getAllClusters());
    }

    @GetMapping("/{id}")
    public ResponseEntity<KafkaClusterResponse> getClusterById(
            @PathVariable UUID id) {

        return ResponseEntity.ok(clusterService.getClusterById(id));
    }

    @GetMapping("/{id}/metadata")
    public KafkaClusterMetadataResponse getMetadata(
            @PathVariable UUID id
    ) {
        return connectionService.getMetadata(id);
    }
}
