package jica.spb.dynamostreams.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * EventType enumeration represents different types of events that can occur in an Amazon DynamoDB Stream.
 * <ol><li><p><code>INSERT_EVENT</code>: Event type for an INSERT operation.</p></li><li><p><code>MODIFY_EVENT</code>: Event type for a MODIFY operation.</p></li><li><p><code>REMOVE_EVENT</code>: Event type for a REMOVE operation.</p></li><li><p><code>NEW_SHARD_EVENT</code>: Event type for a new shard event.</p></li><li><p><code>OLD_SHARD_EVENT</code>: Event type for an old shard event.</p></li></ol>
 */
@Getter
@RequiredArgsConstructor
public enum EventType {

    /**
     * Event type for an INSERT operation.
     */
    INSERT_EVENT("INSERT"),

    /**
     * Event type for a MODIFY operation.
     */
    MODIFY_EVENT("MODIFY"),

    /**
     * Event type for a REMOVE operation.
     */
    REMOVE_EVENT("REMOVE"),

    /**
     * Event type for a new shard event.
     */
    NEW_SHARD_EVENT("NEW_SHARD"),

    /**
     * Event type for an old shard event.
     */
    OLD_SHARD_EVENT("OLD_SHARD");

    /**
     * The value representing the event type.
     */
    final String value;

    /**
     * A list containing all the EventType values.
     */
    public static final List<EventType> ALL_TYPES = List.of(EventType.values());

    /**
     * A list containing the Default Values for Config
     */
    public static final List<EventType> DEFAULT_VALUES = List.of(EventType.INSERT_EVENT, EventType.REMOVE_EVENT, EventType.MODIFY_EVENT);

    /**
     * A map to store the EventType values with their corresponding string representations as keys.
     */
    public static final Map<String, EventType> VALUE_MAP;

    static {
        VALUE_MAP = ALL_TYPES.stream()
                .collect(Collectors.toMap(EventType::getValue, Function.identity()));
    }

}
