
package com.justkafkait.backend.cluster.service;

import com.justkafkait.backend.cluster.dto.CreateKafkaTopicRequest;
import com.justkafkait.backend.cluster.dto.KafkaTopicListItem;
import com.justkafkait.backend.cluster.dto.KafkaTopicMetadataResponse;
import com.justkafkait.backend.cluster.entity.KafkaCluster;
import com.justkafkait.backend.cluster.exception.ClusterNotFoundException;
import com.justkafkait.backend.cluster.kafka.KafkaAdminClientRegistry;
import com.justkafkait.backend.cluster.repository.KafkaClusterRepository;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.http.HttpStatus;
import com.justkafkait.backend.cluster.exception.KafkaTopicException;

@Service
public class KafkaTopicManagementService {

    private final KafkaClusterRepository clusterRepository;
    private final KafkaAdminClientRegistry adminClientRegistry;

    public KafkaTopicManagementService(
            KafkaClusterRepository clusterRepository,
            KafkaAdminClientRegistry adminClientRegistry
    ) {
        this.clusterRepository = clusterRepository;
        this.adminClientRegistry = adminClientRegistry;
    }

    public List<KafkaTopicListItem> listTopics(
            java.util.UUID clusterId
    ) {
        KafkaCluster cluster = getCluster(clusterId);
        AdminClient adminClient = getAdminClient(cluster);

        try {
            return adminClient.listTopics(
                            new ListTopicsOptions().listInternal(false)
                    )
                    .listings()
                    .get(timeout(cluster), TimeUnit.MILLISECONDS)
                    .stream()
                    .map(topic -> new KafkaTopicListItem(
                            topic.name(),
                            topic.isInternal()
                    ))
                    .sorted(java.util.Comparator.comparing(
                            KafkaTopicListItem::name
                    ))
                    .toList();

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Listing Kafka topics was interrupted",
                    exception
            );

        } catch (ExecutionException | TimeoutException exception) {
            throw translateException("list topics", exception);
        }
    }

    public KafkaTopicMetadataResponse getTopic(
            java.util.UUID clusterId,
            String topicName
    ) {
        KafkaCluster cluster = getCluster(clusterId);
        AdminClient adminClient = getAdminClient(cluster);

        try {
            var descriptions = adminClient.describeTopics(
                            List.of(topicName)
                    )
                    .allTopicNames()
                    .get(timeout(cluster), TimeUnit.MILLISECONDS);

            var description = descriptions.get(topicName);

            if (description == null) {
                throw new IllegalStateException(
                        "Kafka returned no description for topic: " + topicName
                );
            }

            List<KafkaTopicMetadataResponse.Partition> partitions =
                    description.partitions()
                            .stream()
                            .map(this::toPartitionResponse)
                            .toList();

            int replicationFactor = partitions.isEmpty()
                    ? 0
                    : partitions.get(0).replicaBrokerIds().size();

            return new KafkaTopicMetadataResponse(
                    description.name(),
                    description.isInternal(),
                    partitions.size(),
                    replicationFactor,
                    partitions
            );

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Getting Kafka topic metadata was interrupted",
                    exception
            );

        } catch (ExecutionException | TimeoutException exception) {
            throw translateException("describe topic", exception);
        }
    }

    public KafkaTopicMetadataResponse createTopic(
            java.util.UUID clusterId,
            CreateKafkaTopicRequest request
    ) {
        validateTopicName(request.name());

        KafkaCluster cluster = getCluster(clusterId);
        AdminClient adminClient = getAdminClient(cluster);

        try {
            NewTopic newTopic = new NewTopic(
                    request.name(),
                    request.partitions(),
                    (short) request.replicationFactor()
            );

            adminClient.createTopics(Set.of(newTopic))
                    .all()
                    .get(timeout(cluster), TimeUnit.MILLISECONDS);

            return getTopic(clusterId, request.name());

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Creating Kafka topic was interrupted",
                    exception
            );

        } catch (ExecutionException | TimeoutException exception) {
            throw translateException("create topic", exception);
        }
    }

    public void deleteTopic(
            java.util.UUID clusterId,
            String topicName
    ) {
        KafkaCluster cluster = getCluster(clusterId);
        AdminClient adminClient = getAdminClient(cluster);

        try {
            adminClient.deleteTopics(List.of(topicName))
                    .all()
                    .get(timeout(cluster), TimeUnit.MILLISECONDS);

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Deleting Kafka topic was interrupted",
                    exception
            );

        } catch (ExecutionException | TimeoutException exception) {
            throw translateException("delete topic", exception);
        }
    }

    private KafkaCluster getCluster(java.util.UUID clusterId) {
        return clusterRepository.findById(clusterId)
                .orElseThrow(() ->
                        new ClusterNotFoundException(clusterId)
                );
    }

    private AdminClient getAdminClient(KafkaCluster cluster) {
        return adminClientRegistry.getOrCreate(cluster);
    }

    private int timeout(KafkaCluster cluster) {
        return cluster.getRequestTimeoutMs();
    }

    private void validateTopicName(String name) {
        if (".".equals(name) || "..".equals(name)) {
            throw new KafkaTopicException(
                    HttpStatus.BAD_REQUEST,
                    "Topic name cannot be '.' or '..'"
            );
        }
    }

    private KafkaTopicMetadataResponse.Partition toPartitionResponse(
            TopicPartitionInfo partition
    ) {
        Integer leaderId = partition.leader() == null
                ? null
                : partition.leader().id();

        List<Integer> replicaIds = partition.replicas()
                .stream()
                .map(node -> node.id())
                .toList();

        List<Integer> inSyncReplicaIds = partition.isr()
                .stream()
                .map(node -> node.id())
                .toList();

        return new KafkaTopicMetadataResponse.Partition(
                partition.partition(),
                leaderId,
                replicaIds,
                inSyncReplicaIds
        );
    }

    private RuntimeException translateException(
            String operation,
            Exception exception
    ) {
        Throwable cause = exception;

        while (cause.getCause() != null) {
            cause = cause.getCause();
        }

        if (cause instanceof TopicExistsException) {
            return new KafkaTopicException(
                    HttpStatus.CONFLICT,
                    "Kafka topic already exists"
            );
        }

        if (cause instanceof UnknownTopicOrPartitionException) {
            return new KafkaTopicException(
                    HttpStatus.NOT_FOUND,
                    "Kafka topic does not exist"
            );
        }

        if (cause instanceof TimeoutException) {
            return new KafkaTopicException(
                    HttpStatus.GATEWAY_TIMEOUT,
                    "Timed out while trying to " + operation + " on Kafka",
                    exception
            );
        }

        return new KafkaTopicException(
                HttpStatus.BAD_GATEWAY,
                "Failed to " + operation + ": " + cause.getMessage(),
                exception
        );
    }
}
