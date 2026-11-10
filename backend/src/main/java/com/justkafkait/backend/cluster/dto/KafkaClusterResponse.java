
package com.justkafkait.backend.cluster.dto;

import com.justkafkait.backend.cluster.entity.SecurityProtocol;

import java.time.Instant;
import java.util.UUID;

public record KafkaClusterResponse(
        UUID id,
        String name,
        String bootstrapServers,
        SecurityProtocol securityProtocol,
        Integer connectionTimeoutMs,
        Integer requestTimeoutMs,
        Instant createdAt,
        Instant updatedAt
) {}
