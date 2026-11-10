
package com.justkafkait.backend.cluster.exception;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

@RestControllerAdvice(basePackages = "com.justkafkait.backend.cluster")
public class ClusterExceptionHandler {

    @ExceptionHandler(ClusterAlreadyExistsException.class)
    public ResponseEntity<ProblemDetail> handleClusterAlreadyExists(
            ClusterAlreadyExistsException ex,
            HttpServletRequest request) {

        return buildProblem(
                HttpStatus.CONFLICT,
                "Cluster Already Exists",
                ex.getMessage(),
                request
        );
    }

    @ExceptionHandler(ClusterNotFoundException.class)
    public ResponseEntity<ProblemDetail> handleClusterNotFound(
            ClusterNotFoundException ex,
            HttpServletRequest request) {

        return buildProblem(
                HttpStatus.NOT_FOUND,
                "Cluster Not Found",
                ex.getMessage(),
                request
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ProblemDetail> handleValidationErrors(
            MethodArgumentNotValidException ex,
            HttpServletRequest request) {

        ProblemDetail problem = createProblem(
                HttpStatus.BAD_REQUEST,
                "Validation Failed",
                "One or more request fields are invalid.",
                request
        );

        Map<String, String> errors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        error -> error.getField(),
                        error -> error.getDefaultMessage() != null
                                ? error.getDefaultMessage()
                                : "Invalid value",
                        (first, second) -> first
                ));

        problem.setProperty("errors", errors);

        return ResponseEntity.badRequest().body(problem);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ProblemDetail> handleInvalidRequestBody(
            HttpMessageNotReadableException ex,
            HttpServletRequest request) {

        return buildProblem(
                HttpStatus.BAD_REQUEST,
                "Invalid Request Body",
                "The request body is missing, malformed, or contains an invalid value.",
                request
        );
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ProblemDetail> handleDataIntegrityViolation(
            DataIntegrityViolationException ex,
            HttpServletRequest request) {

        return buildProblem(
                HttpStatus.CONFLICT,
                "Database Constraint Violation",
                "The request conflicts with an existing database constraint.",
                request
        );
    }

    @ExceptionHandler(KafkaTopicException.class)
    public ProblemDetail handleKafkaTopicException(
            KafkaTopicException exception
    ) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                exception.getStatus(),
                exception.getMessage()
        );

        problem.setTitle("Kafka Topic Operation Failed");

        return problem;
    }

    private ResponseEntity<ProblemDetail> buildProblem(
            HttpStatus status,
            String title,
            String detail,
            HttpServletRequest request) {

        ProblemDetail problem = createProblem(
                status,
                title,
                detail,
                request
        );

        return ResponseEntity.status(status).body(problem);
    }

    private ProblemDetail createProblem(
            HttpStatus status,
            String title,
            String detail,
            HttpServletRequest request) {

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);

        problem.setTitle(title);
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("timestamp", Instant.now());

        return problem;
    }
}
