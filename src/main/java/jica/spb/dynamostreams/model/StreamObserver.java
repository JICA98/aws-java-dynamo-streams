package jica.spb.dynamostreams.model;

/**
 * StreamObserver
 * A functional interface for handling stream events in DynamoDB Streams.
 *
 * @param <T> The type of data associated with the stream events.
 */
@FunctionalInterface
public interface StreamObserver<T> {

    /**
     * This method is a callback method that is invoked when a stream event occurs. It takes a single parameter of type StreamEvent, which contains information about the event in DynamoDB Streams.
     *
     * @param event The StreamEvent containing information about the event in DynamoDB Streams.
     */
    void onChanged(StreamEvent<T> event);

}
