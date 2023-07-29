package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Shard;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@Getter
@RequiredArgsConstructor
public enum EventType {

    InsertEvent,
    ModifyEvent,
    RemoveEvent,
    NewShardsEvent,
    RemoveShardsEvent;

    public static final List<EventType> ALL_TYPES = List.of(

    );

}
