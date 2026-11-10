
package com.justkafkait.backend.cluster.controller;

import com.justkafkait.backend.cluster.dto.CreateKafkaTopicRequest;
import com.justkafkait.backend.cluster.dto.KafkaTopicListItem;
import com.justkafkait.backend.cluster.dto.KafkaTopicMetadataResponse;
import com.justkafkait.backend.cluster.service.KafkaTopicManagementService;

import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/clusters/{clusterId}/topics")
public class KafkaTopicController {

    private final KafkaTopicManagementService topicService;

    public KafkaTopicController(
            KafkaTopicManagementService topicService
    ) {
        this.topicService = topicService;
    }

    @GetMapping
    public List<KafkaTopicListItem> listTopics(
            @PathVariable UUID clusterId
    ) {
        return topicService.listTopics(clusterId);
    }

    @GetMapping("/{topicName}")
    public KafkaTopicMetadataResponse getTopic(
            @PathVariable UUID clusterId,
            @PathVariable String topicName
    ) {
        return topicService.getTopic(clusterId, topicName);
    }

    @PostMapping
    public ResponseEntity<KafkaTopicMetadataResponse> createTopic(
            @PathVariable UUID clusterId,
            @Valid @RequestBody CreateKafkaTopicRequest request
    ) {
        KafkaTopicMetadataResponse response =
                topicService.createTopic(clusterId, request);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(response);
    }

    @DeleteMapping("/{topicName}")
    public ResponseEntity<Void> deleteTopic(
            @PathVariable UUID clusterId,
            @PathVariable String topicName
    ) {
        topicService.deleteTopic(clusterId, topicName);

        return ResponseEntity.noContent().build();
    }
}
