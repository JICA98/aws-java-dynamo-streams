package jica.spb.dynamostreams.model;

import lombok.Value;

/**
 * StreamEvent
 * A class representing an event that occurred in DynamoDB Streams.
 *
 * @param <T> The type of data associated with the event.
 */
@Value
public class StreamEvent<T> {

    /**
     * The type of the event.
     */
    EventType eventType;

    /**
     * The DynamoRecord associated with the event.
     */
    DynamoRecord<T> dynamoRecord;

    /**
     * The StreamShards associated with the event.
     */
    StreamShards shards;

    /**
     * StreamEvent constructor.
     *
     * @param eventType The type of the event.
     * @param dynamoRecord The DynamoRecord associated with the event.
     * @param shards The StreamShards associated with the event.
     */
    public StreamEvent(EventType eventType, DynamoRecord<T> dynamoRecord, StreamShards shards) {
        this.eventType = eventType;
        this.dynamoRecord = dynamoRecord;
        this.shards = shards;
    }
}
