
package com.justkafkait.backend.cluster.service;

import com.justkafkait.backend.cluster.dto.KafkaClusterMetadataResponse;
import com.justkafkait.backend.cluster.entity.KafkaCluster;
import com.justkafkait.backend.cluster.exception.ClusterNotFoundException;
import com.justkafkait.backend.cluster.kafka.KafkaAdminClientRegistry;
import com.justkafkait.backend.cluster.repository.KafkaClusterRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaClusterConnectionService {

    private final KafkaClusterRepository repository;
    private final KafkaAdminClientRegistry registry;

    public KafkaClusterConnectionService(
            KafkaClusterRepository repository,
            KafkaAdminClientRegistry registry
    ) {
        this.repository = repository;
        this.registry = registry;
    }

    public KafkaClusterMetadataResponse getMetadata(UUID clusterId) {
        KafkaCluster cluster = repository.findById(clusterId)
                .orElseThrow(() -> new ClusterNotFoundException(clusterId));

        AdminClient adminClient = registry.getOrCreate(cluster);

        try {
            DescribeClusterResult result = adminClient.describeCluster();

            int timeout = cluster.getRequestTimeoutMs();

            String kafkaClusterId = result.clusterId()
                    .get(timeout, TimeUnit.MILLISECONDS);

            List<KafkaClusterMetadataResponse.BrokerNode> brokers =
                    result.nodes()
                            .get(timeout, TimeUnit.MILLISECONDS)
                            .stream()
                            .map(this::toBrokerNode)
                            .toList();

            return new KafkaClusterMetadataResponse(
                    cluster.getId(),
                    kafkaClusterId,
                    brokers
            );

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Kafka metadata request was interrupted",
                    exception
            );

        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Unable to connect to Kafka cluster: "
                            + cluster.getName(),
                    exception
            );
        }
    }

    private KafkaClusterMetadataResponse.BrokerNode toBrokerNode(Node node) {
        return new KafkaClusterMetadataResponse.BrokerNode(
                node.id(),
                node.host(),
                node.port(),
                node.rack()
        );
    }
}
