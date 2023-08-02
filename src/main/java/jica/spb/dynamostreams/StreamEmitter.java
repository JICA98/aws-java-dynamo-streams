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

/**
 * StreamEmitter
 * <p>
 * A utility class that emits and processes DynamoDB stream events and provides various Flux-based methods to work with the emitted data.
 *
 * @param <T> The type of data encapsulated in DynamoRecord.
 */
public class StreamEmitter<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PATH_DELIMITER = "/";

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final Sinks.Many<StreamEvent<T>> sink = Sinks.many().multicast().onBackpressureBuffer();

    /**
     * Emits a StreamEvent to the internal sink for further processing.
     *
     * @param event The StreamEvent to be emitted.
     */
    protected void emit(StreamEvent<T> event) {
        sink.tryEmitNext(event);
    }

    /**
     * Converts the internal sink to a Flux, allowing the consumption of StreamEvent elements.
     *
     * @return A Flux representing the stream of StreamEvent elements.
     */
    public Flux<StreamEvent<T>> asFlux() {
        return sink.asFlux();
    }

    /**
     * Returns a Flux containing DynamoRecord elements extracted from the StreamEvent elements.
     *
     * @return A Flux representing the stream of DynamoRecord elements.
     */
    public Flux<DynamoRecord<T>> records() {
        return asFlux().map(StreamEvent::getDynamoRecord);
    }

    /**
     * Returns a Flux containing non-null DynamoRecord elements extracted from the StreamEvent elements.
     *
     * @return A Flux representing the stream of non-null DynamoRecord elements.
     */
    public Flux<DynamoRecord<T>> nonNullRecords() {
        return records().filter(Objects::nonNull);
    }

    /**
     * Returns a Flux containing newImage elements extracted from the non-null DynamoRecord elements.
     *
     * @return A Flux representing the stream of newImage elements.
     */
    public Flux<T> newImages() {
        return nonNullRecords().map(DynamoRecord::getNewImage);
    }

    /**
     * Filters the newImage elements using the specified predicate.
     *
     * @param predicate The predicate used for filtering the newImage elements.
     * @return A Flux representing the filtered stream of newImage elements.
     */
    public Flux<T> filterNewImages(Predicate<T> predicate) {
        return newImages().filter(predicate);
    }

    /**
     * Returns a Flux containing oldImage elements extracted from the non-null DynamoRecord elements.
     *
     * @return A Flux representing the stream of oldImage elements.
     */
    public Flux<T> oldImages() {
        return nonNullRecords().map(DynamoRecord::getOldImage);
    }

    /**
     * Filters the oldImage elements using the specified predicate.
     *
     * @param predicate The predicate used for filtering the oldImage elements.
     * @return A Flux representing the filtered stream of oldImage elements.
     */
    public Flux<T> filterOldImages(Predicate<T> predicate) {
        return oldImages().filter(predicate);
    }

    /**
     * Returns a Flux containing keyValues elements extracted from the non-null DynamoRecord elements.
     *
     * @return A Flux representing the stream of keyValues elements.
     */
    public Flux<T> keyValues() {
        return nonNullRecords().map(DynamoRecord::getKeyValues);
    }

    /**
     * Filters the keyValues elements using the specified predicate.
     *
     * @param predicate The predicate used for filtering the keyValues elements.
     * @return A Flux representing the filtered stream of keyValues elements.
     */
    public Flux<T> filterKeyValues(Predicate<T> predicate) {
        return keyValues().filter(predicate);
    }

    /**
     * Returns a Flux containing the original DynamoDB stream records extracted from the non-null DynamoRecord elements.
     *
     * @return A Flux representing the stream of original DynamoDB stream records (Record).
     */
    public Flux<Record> originalRecords() {
        return nonNullRecords().map(DynamoRecord::getOriginalRecord);
    }

    /**
     * Returns a Flux containing the value referenced by the specified path in the newImage of the DynamoDB stream records.
     *
     * @param reference The reference path to the desired value in the newImage.
     * @param clazz     The class type to which the value should be converted.
     * @param <R>       The type of the value to be extracted and converted.
     * @return A Flux representing the stream of Optional values corresponding to the referenced path in the newImage.
     */
    public <R> Flux<Optional<R>> newImageRef(String reference, Class<R> clazz) {
        return originalRecords()
                .map(dynRecords -> ItemUtils.toItem(dynRecords.getDynamodb().getNewImage()).asMap())
                .map(map -> findReference(reference, map, clazz));
    }

    /**
     * Returns a Flux containing the value referenced by the specified path in the oldImage of the DynamoDB stream records.
     *
     * @param reference The reference path to the desired value in the oldImage.
     * @param clazz     The class type to which the value should be converted.
     * @param <R>       The type of the value to be extracted and converted.
     * @return A Flux representing the stream of Optional values corresponding to the referenced path in the oldImage.
     */
    public <R> Flux<Optional<R>> oldImageRef(String reference, Class<R> clazz) {
        return originalRecords()
                .map(dynRecords -> ItemUtils.toItem(dynRecords.getDynamodb().getOldImage()).asMap())
                .map(map -> findReference(reference, map, clazz));
    }

    /**
     * Returns a Flux containing the map referenced by the specified path in the newImage of the DynamoDB stream records.
     *
     * @param reference The reference path to the desired map in the newImage.
     * @return A Flux representing the stream of Optional maps corresponding to the referenced path in the newImage.
     */
    public Flux<Optional<Map<String, Object>>> newImageRef(String reference) {
        return newImageRef(reference, null);
    }

    /**
     * Returns a Flux containing the map referenced by the specified path in the oldImage of the DynamoDB stream records.
     *
     * @param reference The reference path to the desired map in the oldImage.
     * @return A Flux representing the stream of Optional maps corresponding to the referenced path in the oldImage.
     */
    public Flux<Optional<Map<String, Object>>> oldImageRef(String reference) {
        return oldImageRef(reference, null);
    }

    /**
     * Recursive method to find the value referenced by the specified path in a nested map.
     *
     * @param reference The reference path to the desired value.
     * @param map       The map containing the nested structure.
     * @param clazz     The class type to which the value should be converted.
     * @param <R>       The type of the value to be extracted and converted.
     * @return An Optional containing the extracted value, or an empty Optional if the path is not found.
     */
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

    /**
     * Recursive method to extract the value from a nested structure when it exists.
     *
     * @param clazz The class type to which the value should be converted.
     * @param entry The entry containing the nested structure.
     * @param split The list of path elements.
     * @param <R>   The type of the value to be extracted and converted.
     * @return An Optional containing the extracted value, or an empty Optional if the path is not found.
     */
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

    /**
     * Helper method to safely cast an object to a specific type.
     *
     * @param object The object to be casted.
     * @param <R>    The type to which the object should be casted.
     * @return The casted object, or null if the casting fails.
     */
    @SuppressWarnings("unchecked")
    private <R> R cast(Object object) {
        R typedObject;

        try {
            typedObject = (R) object;
        } catch (ClassCastException ignored) {
            typedObject = null;
        }

        return typedObject;
    }
}
