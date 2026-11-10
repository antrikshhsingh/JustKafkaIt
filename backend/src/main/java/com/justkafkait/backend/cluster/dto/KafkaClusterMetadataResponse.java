
package com.justkafkait.backend.cluster.dto;

import java.util.List;
import java.util.UUID;

public record KafkaClusterMetadataResponse(
        UUID clusterId,
        String kafkaClusterId,
        List<BrokerNode> brokers
) {
    public record BrokerNode(
            int id,
            String host,
            int port,
            String rack
    ) {}
}
