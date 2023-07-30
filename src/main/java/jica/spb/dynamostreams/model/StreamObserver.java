package jica.spb.dynamostreams.model;

@FunctionalInterface
public interface StreamObserver<T> {

    void onChanged(StreamEvent<T> event);

}
