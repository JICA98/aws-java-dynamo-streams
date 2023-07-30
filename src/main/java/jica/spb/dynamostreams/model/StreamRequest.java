package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import lombok.Builder;
import lombok.Value;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * StreamRequest
 * @param <T>
 */
@Value
@Builder
public class StreamRequest<T> {

    Class<T> type;

    String streamARN;

    AmazonDynamoDBStreams dynamoDBStreams;

    Executor executor;

    Function<Map<String, AttributeValue>, T> mapperFn;

    List<EventType> eventTypes;

    ShardIteratorType shardIteratorType;

    PollConfig pollConfig;

    public StreamRequest(
            Class<T> type, String streamARN,
            AmazonDynamoDBStreams dynamoDBStreams,
            Executor executor,
            Function<Map<String, AttributeValue>, T> mapperFn,
            List<EventType> eventTypes, ShardIteratorType shardIteratorType,
            PollConfig pollConfig
    ) {

        Objects.requireNonNull(type);
        Objects.requireNonNull(streamARN);
        Objects.requireNonNull(dynamoDBStreams);

        this.type = type;
        this.streamARN = streamARN;
        this.dynamoDBStreams = dynamoDBStreams;
        this.executor = executor;
        boolean isObjectClass = type == Object.class;

        this.shardIteratorType = Objects.requireNonNullElse(shardIteratorType, ShardIteratorType.LATEST);
        this.eventTypes = Objects.requireNonNullElse(eventTypes, EventType.ALL_TYPES);
        this.mapperFn = getObjectMapper(mapperFn, isObjectClass);
        this.pollConfig = Objects.requireNonNullElse(pollConfig, new PollConfig());
    }

    private Function<Map<String, AttributeValue>, T> getObjectMapper(
            Function<Map<String, AttributeValue>, T> mapperFn,
            boolean isObjectClass
    ) {
        if (mapperFn == null) {
            if (isObjectClass) {
                return this::defaultMapper;
            } else {
                throw new IllegalArgumentException("No mapper provided for custom class");
            }
        } else {
            return mapperFn;
        }
    }

    @SuppressWarnings("unchecked")
    private T defaultMapper(Map<String, AttributeValue> value) {
        return (T) Objects.requireNonNullElse(value, Collections.emptyMap());
    }

}
