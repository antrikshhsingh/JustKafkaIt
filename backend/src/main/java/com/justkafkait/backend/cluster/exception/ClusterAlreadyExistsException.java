
package com.justkafkait.backend.cluster.exception;

public class ClusterAlreadyExistsException extends RuntimeException {

    public ClusterAlreadyExistsException(String name) {
        super("A Kafka cluster with name '" + name + "' already exists");
    }
}
