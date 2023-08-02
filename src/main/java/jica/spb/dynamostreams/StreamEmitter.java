package jica.spb.dynamostreams;

import com.amazonaws.services.dynamodbv2.model.Record;
import jica.spb.dynamostreams.model.DynamoRecord;
import jica.spb.dynamostreams.model.StreamEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Objects;
import java.util.function.Predicate;

public class StreamEmitter<T> {

    private final Sinks.Many<StreamEvent<T>> sink = Sinks.many().multicast().onBackpressureBuffer();

    protected void emit(StreamEvent<T> event) {
        sink.tryEmitNext(event);
    }

    public Flux<StreamEvent<T>> asFlux() {
        return sink.asFlux();
    }

    public Flux<DynamoRecord<T>> records() {
        return asFlux().map(StreamEvent::getDynamoRecord);
    }

    public Flux<DynamoRecord<T>> nonNullRecords() {
        return records().filter(Objects::nonNull);
    }

    public Flux<T> newImages() {
        return nonNullRecords().map(DynamoRecord::getNewImage);
    }

    public Flux<T> filterNewImages(Predicate<T> predicate) {
        return newImages().filter(predicate);
    }

    public Flux<T> oldImages() {
        return nonNullRecords().map(DynamoRecord::getOldImage);
    }

    public Flux<T> filterOldImages(Predicate<T> predicate) {
        return oldImages().filter(predicate);
    }

    public Flux<T> keyValues() {
        return nonNullRecords().map(DynamoRecord::getKeyValues);
    }

    public Flux<T> filterKeyValues(Predicate<T> predicate) {
        return keyValues().filter(predicate);
    }

    public Flux<Record> originalRecords() {
        return nonNullRecords().map(DynamoRecord::getOriginalRecord);
    }
}
