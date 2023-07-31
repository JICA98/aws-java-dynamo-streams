package jica.spb.dynamostreams.config;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import jica.spb.dynamostreams.model.EventType;
import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * The StreamConfig class represents a configuration for setting up a subscription to an Amazon DynamoDB stream. It encapsulates various parameters required for the subscription, including the Amazon DynamoDB Streams client, stream ARN, event mapper, shard iterator type, and polling configuration.
 *
 * @param <T> The type of data that will be processed from the stream records. This is defined by the user when creating an instance of StreamConfig.
 */
@Value
@Builder(toBuilder = true)
public class StreamConfig<T> {

    /**
     * The class representing the type of data to be processed from the stream records.
     */
    Class<T> clazz;

    /**
     * The ARN (Amazon Resource Name) of the DynamoDB stream to subscribe to.
     * streamARN The ARN of the DynamoDB stream.
     */
    String streamARN;

    /**
     * An instance of AmazonDynamoDBStreams representing the DynamoDB Streams client.
     */
    AmazonDynamoDBStreams dynamoDBStreams;

    /**
     * An implementation of the Executor interface to handle asynchronous operations during the stream subscription.
     */
    Executor executor;

    /**
     * A list of EventType values representing the types of stream events to process.
     */
    List<EventType> eventTypes;

    /**
     * The type of shard iterator to use when fetching records from the stream.
     */
    ShardIteratorType shardIteratorType;

    /**
     * A PollConfig object containing the configuration for polling-based stream subscription.
     */
    PollConfig pollConfig;

    /**
     * Configuration for handling mapping and marshalling of DynamoDB stream records to objects of type T.
     */
    MapperConfig<T> mapperConfig;

    /**
     * StreamConfig constructor.
     * Parameters
     * <ul><li><code>type</code>: The <code>Class&lt;T&gt;</code> representing the type of data to be processed from the stream records.</li>
     * <li><code>streamARN</code>: The ARN (Amazon Resource Name) of the DynamoDB stream to subscribe to.</li>
     * <li><code>dynamoDBStreams</code>: An instance of <code>AmazonDynamoDBStreams</code> representing the DynamoDB Streams client.</li>
     * <li><code>executor</code>: An implementation of the <code>Executor</code> interface to handle asynchronous operations during the stream subscription.</li>
     * <li><code>mapperFn</code>: A function that maps the <code>Map&lt;String, AttributeValue&gt;</code>
     * representation of a record from the stream to an object of type <code>T</code>.</li>
     * <li><code>eventTypes</code>: A list of <code>EventType</code>
     * values representing the types of stream events to process. If not specified,
     * EventType.INSERT_EVENT, EventType.REMOVE_EVENT, EventType.MODIFY_EVENT will be processed.</li>
     * <li><code>shardIteratorType</code>: The type of shard iterator to use when fetching records from the stream. If not specified,
     * <code>ShardIteratorType.LATEST</code> will be used.</li><li><code>pollConfig</code>: A
     * <code>PollConfig</code> object containing the configuration for polling-based stream subscription. If not specified, default polling configurations will be used.</li></ul>
     * <p>
     * This constructor creates an instance of StreamConfig with the provided configuration.
     * It validates and initializes all the required parameters, and provides default values for optional parameters if not specified.
     *
     * @param clazz             The class representing the type of data to be processed from the stream records.
     * @param streamARN         The ARN of the DynamoDB stream.
     * @param dynamoDBStreams   The AmazonDynamoDBStreams client.
     * @param executor          The Executor for handling asynchronous operations.
     * @param eventTypes        The list of event types to process.
     * @param shardIteratorType The shard iterator type.
     * @param pollConfig        The PollConfig for polling-based subscription.
     * @param mapperConfig      The MapperConfig
     */
    @SuppressWarnings("unchecked")
    public StreamConfig(
            Class<T> clazz, String streamARN,
            AmazonDynamoDBStreams dynamoDBStreams,
            Executor executor,
            List<EventType> eventTypes,
            ShardIteratorType shardIteratorType,
            PollConfig pollConfig,
            MapperConfig<T> mapperConfig) {

        Objects.requireNonNull(dynamoDBStreams);

        this.streamARN = streamARN;
        this.dynamoDBStreams = dynamoDBStreams;
        this.executor = executor;

        this.clazz = (Class<T>) Objects.requireNonNullElse(clazz, Object.class);
        this.shardIteratorType = Objects.requireNonNullElse(shardIteratorType, ShardIteratorType.LATEST);
        this.eventTypes = Objects.requireNonNullElse(eventTypes, EventType.DEFAULT_VALUES);
        this.pollConfig = Objects.requireNonNullElse(pollConfig, new PollConfig());
        this.mapperConfig = Objects.requireNonNullElse(mapperConfig, new MapperConfig<>(clazz));
    }

}
