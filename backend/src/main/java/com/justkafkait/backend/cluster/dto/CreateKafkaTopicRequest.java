
package com.justkafkait.backend.cluster.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record CreateKafkaTopicRequest(

        @NotBlank(message = "Topic name is required")
        @Size(max = 249, message = "Topic name cannot exceed 249 characters")
        @Pattern(
                regexp = "^[a-zA-Z0-9._-]+$",
                message = "Topic name may only contain letters, numbers, '.', '_' and '-'"
        )
        String name,

        @Min(value = 1, message = "At least one partition is required")
        int partitions,

        @Min(value = 1, message = "Replication factor must be at least 1")
        @Max(value = 32767, message = "Replication factor is too large")
        int replicationFactor

) {}
