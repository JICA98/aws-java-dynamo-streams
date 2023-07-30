package jica.spb.dynamostreams.model;

import lombok.Value;

@Value
public class StreamEvent<T> {

    EventType eventType;

    DynamoRecord<T> dynamoRecord;

    StreamShards shards;

}
