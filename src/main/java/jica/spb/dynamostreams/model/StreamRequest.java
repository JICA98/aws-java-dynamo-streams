package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import lombok.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

@Data
@Builder
public class StreamRequest<T> {

    private final String streamARN;

    private final AmazonDynamoDBStreams dynamoDBStreams;

    private final ExecutorService executorService;

    private final Function<Map<String, AttributeValue>, T> mapperFn;

    private final List<EventType> eventTypes;

    private final ShardIteratorType shardIteratorType;

    private StreamRequest(
            String streamARN,
            AmazonDynamoDBStreams dynamoDBStreams,
            ExecutorService executorService,
            Function<Map<String, AttributeValue>, T> mapperFn,
            List<EventType> eventTypes, ShardIteratorType shardIteratorType) {

        Objects.requireNonNull(streamARN);
        Objects.requireNonNull(dynamoDBStreams);

        this.streamARN = streamARN;
        this.dynamoDBStreams = dynamoDBStreams;
        this.executorService = executorService;
        this.mapperFn = mapperFn;

        this.shardIteratorType = Objects.requireNonNullElse(shardIteratorType, ShardIteratorType.LATEST);
        this.eventTypes = Objects.requireNonNullElse(eventTypes, EventType.ALL_TYPES);
    }

}
