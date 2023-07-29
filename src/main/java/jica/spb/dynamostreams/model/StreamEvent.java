package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Shard;

public class EventValue {

    private static EventType<T> insertEvent;
    private EventType<T> modifyEvent;
    private EventType<T> removeEvent;
    private EventType<Shard> newShardsEvent;
    private EventType<Shard> removeShardsEvent;

}
