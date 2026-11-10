
package com.justkafkait.backend.cluster.repository;

import com.justkafkait.backend.cluster.entity.KafkaCluster;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface KafkaClusterRepository extends JpaRepository<KafkaCluster, UUID> {

    Optional<KafkaCluster> findByName(String name);

    boolean existsByName(String name);
}
