
package com.justkafkait.backend.cluster.exception;

import java.util.UUID;

public class ClusterNotFoundException extends RuntimeException {

    public ClusterNotFoundException(UUID id) {
        super("Kafka cluster not found with ID: " + id);
    }
}
