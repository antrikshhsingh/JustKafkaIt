
package com.justkafkait.backend.cluster.exception;

import org.springframework.http.HttpStatus;

public class KafkaTopicException extends RuntimeException {

    private final HttpStatus status;

    public KafkaTopicException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public KafkaTopicException(
            HttpStatus status,
            String message,
            Throwable cause
    ) {
        super(message, cause);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
