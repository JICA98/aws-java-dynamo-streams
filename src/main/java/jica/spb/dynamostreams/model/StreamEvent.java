package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Shard;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
public class StreamEvent<T> {

    EventType eventType;

    T value;

}
