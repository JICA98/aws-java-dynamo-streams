package jica.spb.dynamostreams;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jica.spb.dynamostreams.model.DynamoRecord;
import jica.spb.dynamostreams.model.StreamEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.*;
import java.util.function.Predicate;

public class StreamEmitter<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String PATH_DELIMITER = "/";

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

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

    public <R> Flux<Optional<R>> newImageRef(String reference, Class<R> clazz) {
        return originalRecords()
                .map(dynRecords -> ItemUtils.toItem(dynRecords.getDynamodb().getNewImage()).asMap())
                .map(map -> findReference(reference, map, clazz));
    }

    public <R> Flux<Optional<R>> oldImageRef(String reference, Class<R> clazz) {
        return originalRecords()
                .map(dynRecords -> ItemUtils.toItem(dynRecords.getDynamodb().getOldImage()).asMap())
                .map(map -> findReference(reference, map, clazz));
    }

    public Flux<Optional<Map<String, Object>>> newImageRef(String reference) {
        return newImageRef(reference, null);
    }

    public Flux<Optional<Map<String, Object>>> oldImageRef(String reference) {
        return oldImageRef(reference, null);
    }

    private <R> Optional<R> findReference(String reference, Map<String, Object> map, Class<R> clazz) {
        if (map == null || map.isEmpty()) {
            return Optional.empty();
        }
        List<String> split = new ArrayList<>(List.of(reference.split(PATH_DELIMITER)));
        String top = split.get(0);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (top.equals(entry.getKey())) {
                if (entry.getValue() == null) {
                    return Optional.empty();
                } else {
                    return whenValueExists(clazz, entry, split);
                }
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private <R> Optional<R> whenValueExists(Class<R> clazz, Map.Entry<String, Object> entry, List<String> split) {
        if (split.size() == 1) {
            if (clazz == null) {
                return (Optional<R>) Optional.of(entry.getValue());
            } else {
                return Optional.of(OBJECT_MAPPER.convertValue(entry.getValue(), clazz));
            }
        } else {
            split.remove(0);
            return findReference(String.join(PATH_DELIMITER, split), cast(entry.getValue()), clazz);
        }
    }

    @SuppressWarnings("unchecked")
    private <R> R cast(Object object) {
        R typedObject = null;

        try {
            typedObject = (R) object;
        } catch (ClassCastException ignored) {
            typedObject = null;
        }

        return typedObject;
    }
}
