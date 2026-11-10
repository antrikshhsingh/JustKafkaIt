
package com.justkafkait.backend.cluster.service;

import com.justkafkait.backend.cluster.dto.CreateKafkaClusterRequest;
import com.justkafkait.backend.cluster.dto.KafkaClusterResponse;
import com.justkafkait.backend.cluster.entity.KafkaCluster;
import com.justkafkait.backend.cluster.exception.ClusterAlreadyExistsException;
import com.justkafkait.backend.cluster.exception.ClusterNotFoundException;
import com.justkafkait.backend.cluster.repository.KafkaClusterRepository;

import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@Transactional(readOnly = true)
public class KafkaClusterService {

    private final KafkaClusterRepository clusterRepository;

    public KafkaClusterService(KafkaClusterRepository clusterRepository) {
        this.clusterRepository = clusterRepository;
    }

    @Transactional
    public KafkaClusterResponse createCluster(CreateKafkaClusterRequest request) {

        String clusterName = request.name().trim();

        if (clusterRepository.existsByName(clusterName)) {
            throw new ClusterAlreadyExistsException(clusterName);
        }

        KafkaCluster cluster = new KafkaCluster();

        cluster.setName(clusterName);
        cluster.setBootstrapServers(request.bootstrapServers().trim());
        cluster.setSecurityProtocol(request.securityProtocol());
        cluster.setConnectionTimeoutMs(request.connectionTimeoutMs());
        cluster.setRequestTimeoutMs(request.requestTimeoutMs());

        KafkaCluster savedCluster = clusterRepository.save(cluster);

        return mapToResponse(savedCluster);
    }

    public List<KafkaClusterResponse> getAllClusters() {

        return clusterRepository.findAll(Sort.by(Sort.Direction.ASC, "name"))
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    public KafkaClusterResponse getClusterById(UUID id) {

        KafkaCluster cluster = clusterRepository.findById(id)
                .orElseThrow(() -> new ClusterNotFoundException(id));

        return mapToResponse(cluster);
    }

    private KafkaClusterResponse mapToResponse(KafkaCluster cluster) {

        return new KafkaClusterResponse(
                cluster.getId(),
                cluster.getName(),
                cluster.getBootstrapServers(),
                cluster.getSecurityProtocol(),
                cluster.getConnectionTimeoutMs(),
                cluster.getRequestTimeoutMs(),
                cluster.getCreatedAt(),
                cluster.getUpdatedAt()
        );
    }
}
