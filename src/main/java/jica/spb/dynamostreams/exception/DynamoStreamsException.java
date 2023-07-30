package jica.spb.dynamostreams.exception;

/**
 * DynamoStreamsException
 * The DynamoStreamsException class is a custom exception used for handling errors related to DynamoDB Streams processing within the application.
 * Custom exception class for handling errors related to DynamoDB Streams processing.
 */
public class DynamoStreamsException extends RuntimeException {

    /**
     * Constructor for creating a DynamoStreamsException with a message and a cause.
     *
     * @param message The detail message describing the exception.
     * @param cause The cause of the exception.
     */
    public DynamoStreamsException(String message, Throwable cause) {
        super(message, cause);
    }
}
