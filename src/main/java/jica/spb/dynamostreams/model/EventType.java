package jica.spb.dynamostreams.model;

import com.amazonaws.transform.MapEntry;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
public enum EventType {

    INSERT_EVENT("INSERT"),
    MODIFY_EVENT("MODIFY"),
    REMOVE_EVENT("REMOVE"),
    NEW_SHARD_EVENT("NEW_SHARD"),
    OLD_SHARD_EVENT("OLD_SHARD");

    final String value;

    public static final List<EventType> ALL_TYPES = List.of(EventType.values());

    public static final Map<String, EventType> VALUE_MAP;

    static {
        VALUE_MAP = ALL_TYPES.stream()
                .collect(Collectors.toMap(EventType::getValue, Function.identity()));
    }

}
