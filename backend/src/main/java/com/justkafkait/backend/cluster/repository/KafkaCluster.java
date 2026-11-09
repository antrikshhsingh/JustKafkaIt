package com.justkafkait.backend.cluster.repository;

import com.justkafkait.backend.cluster.entity.SecurityProtocol;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "kafka_clusters")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaCluster {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "name", nullable = false, length = 100)
    private String name;

    @Column(name = "bootstrap_servers", nullable = false, length = 1000)
    private String bootstrapServers;

    @Enumerated(EnumType.STRING)
    @Column(name = "security_protocol", nullable = false, length = 30)
    private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;

    @Column(name = "connection_timeout_ms", nullable = false)
    private Integer connectionTimeoutMs = 5000;

    @Column(name = "request_timeout_ms", nullable = false)
    private Integer requestTimeoutMs = 10000;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
