
package com.justkafkait.backend.cluster.dto;

import java.util.List;

public record KafkaTopicMetadataResponse(
        String name,
        boolean internal,
        int partitionCount,
        int replicationFactor,
        List<Partition> partitions
) {
    public record Partition(
            int partitionId,
            Integer leaderBrokerId,
            List<Integer> replicaBrokerIds,
            List<Integer> inSyncReplicaBrokerIds
    ) {}
}
