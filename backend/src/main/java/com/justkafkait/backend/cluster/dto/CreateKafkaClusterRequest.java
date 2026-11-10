
package com.justkafkait.backend.cluster.dto;

import com.justkafkait.backend.cluster.entity.SecurityProtocol;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record CreateKafkaClusterRequest(

        @NotBlank(message = "Cluster name is required")
        @Size(max = 100, message = "Cluster name cannot exceed 100 characters")
        String name,

        @NotBlank(message = "Bootstrap servers are required")
        @Size(max = 1000, message = "Bootstrap servers cannot exceed 1000 characters")
        String bootstrapServers,

        @NotNull(message = "Security protocol is required")
        SecurityProtocol securityProtocol,

        @NotNull
        @Min(1000)
        @Max(120000)
        Integer connectionTimeoutMs,

        @NotNull
        @Min(1000)
        @Max(120000)
        Integer requestTimeoutMs

) {}
