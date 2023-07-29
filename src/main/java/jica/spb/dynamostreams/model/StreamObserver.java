package jica.spb.dynamostreams.model;

public interface StreamObserver {

    <T> void onChanged(StreamEvent<T> event);

}
