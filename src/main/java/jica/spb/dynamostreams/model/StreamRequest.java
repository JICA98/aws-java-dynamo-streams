package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;

import java.util.Collections;

import lombok.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Function;

@Data
@Builder
public class StreamRequest<T> {

    private static final int STREAM_DESCRIPTION_LIMIT = 10;

    private final Class<T> type;

    private final String streamARN;

    private final AmazonDynamoDBStreams dynamoDBStreams;

    private final Executor executor;

    private final Function<Map<String, AttributeValue>, T> mapperFn;

    private final List<EventType> eventTypes;

    private final ShardIteratorType shardIteratorType;

    private final int streamDescriptionLimitPerPoll;

    public StreamRequest(
            Class<T> type, String streamARN,
            AmazonDynamoDBStreams dynamoDBStreams,
            Executor executor,
            Function<Map<String, AttributeValue>, T> mapperFn,
            List<EventType> eventTypes, ShardIteratorType shardIteratorType,
            Integer streamDescriptionLimitPerPoll) {

        Objects.requireNonNull(type);
        Objects.requireNonNull(streamARN);
        Objects.requireNonNull(dynamoDBStreams);

        this.type = type;
        this.streamARN = streamARN;
        this.dynamoDBStreams = dynamoDBStreams;
        this.executor = executor;
        boolean isObjectClass = type == Object.class;

        this.shardIteratorType = Objects.requireNonNullElse(shardIteratorType, ShardIteratorType.LATEST);
        this.streamDescriptionLimitPerPoll = Objects.requireNonNullElse(streamDescriptionLimitPerPoll, STREAM_DESCRIPTION_LIMIT);
        this.eventTypes = Objects.requireNonNullElse(eventTypes, EventType.ALL_TYPES);
        this.mapperFn = getObjectMapper(mapperFn, isObjectClass);
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
