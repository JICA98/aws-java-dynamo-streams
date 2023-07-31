package jica.spb.dynamostreams.config;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Value;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * MapperConfig
 * A configuration class for handling mapping and marshalling of DynamoDB stream records to objects of type T.
 *
 * @param <T> The type of data to be processed from the stream records.
 */
@Value
@Builder(toBuilder = true)
public class MapperConfig<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * The class representing the type of data to be processed from the stream records.
     */
    Class<T> clazz;

    /**
     * A function that maps the Map of String to AttributeValue representation of a record from the stream to an object of type T.
     */
    Function<Map<String, AttributeValue>, T> mapperFn;

    /**
     * The custom DynamoDBMapper used for custom mapping of stream records.
     */
    DynamoDBMapper dynamoDBMapper;

    /**
     * The custom DynamoDBMapperConfig used for custom mapping configurations.
     */
    DynamoDBMapperConfig dynamoDBMapperConfig;

    MapperConfig(Class<T> clazz) {
        this(clazz, null, null, null);
    }

    /**
     * MapperConfig constructor.
     *
     * @param clazz                The class representing the type of data to be processed from the stream records.
     * @param mapperFn             A function that maps the Map of String to AttributeValue representation of a record from the stream to an object of type T.
     * @param dynamoDBMapper       The DynamoDBMapper used for custom mapping.
     * @param dynamoDBMapperConfig The DynamoDBMapperConfig used for custom mapping configurations.
     */
    @SuppressWarnings("unchecked")
    public MapperConfig(
            Class<T> clazz,
            Function<Map<String, AttributeValue>, T> mapperFn,
            DynamoDBMapper dynamoDBMapper,
            DynamoDBMapperConfig dynamoDBMapperConfig) {
        this.clazz = (Class<T>) Objects.requireNonNullElse(clazz, Object.class);
        this.dynamoDBMapper = dynamoDBMapper;
        this.dynamoDBMapperConfig = dynamoDBMapperConfig;

        boolean isObjectClass = clazz == Object.class;
        this.mapperFn = getObjectMapper(mapperFn, isObjectClass);
    }

    /**
     * Get the mapper function based on the provided mapping options.
     *
     * @param mapperFn      The custom mapper function, if provided.
     * @param isObjectClass Flag indicating if the class is the generic Object class.
     * @return The appropriate mapper function for the given mapping options.
     */
    private Function<Map<String, AttributeValue>, T> getObjectMapper(
            Function<Map<String, AttributeValue>, T> mapperFn,
            boolean isObjectClass
    ) {
        if (mapperFn == null) {
            if (isObjectClass) {
                return this::defaultMapper;
            } else {
                if (dynamoDBMapper == null) {
                    return this::objectMapper;
                } else {
                    return this::dynamoDBMapper;
                }
            }
        } else {
            return mapperFn;
        }
    }

    /**
     * Marshalls the AttributeValue map using the provided DynamoDBMapper.
     *
     * @param valueMap The AttributeValue map to be marshalled.
     * @return The object of type T after marshalling.
     */
    private T dynamoDBMapper(Map<String, AttributeValue> valueMap) {
        if (valueMap == null) {
            return null;
        } else {
            if (dynamoDBMapperConfig == null) {
                return dynamoDBMapper.marshallIntoObject(clazz, valueMap);
            } else {
                return dynamoDBMapper.marshallIntoObject(clazz, valueMap, dynamoDBMapperConfig);
            }
        }
    }

    /**
     * Converts the AttributeValue map using Jackson's ObjectMapper.
     *
     * @param valueMap The AttributeValue map to be converted.
     * @return The object of type T after conversion.
     */
    private T objectMapper(Map<String, AttributeValue> valueMap) {
        if (valueMap == null) {
            return null;
        } else {
            return OBJECT_MAPPER.convertValue(ItemUtils.toItem(valueMap).asMap(), clazz);
        }
    }

    /**
     * Default mapper function used when a custom mapper is not provided.
     *
     * @param value The AttributeValue map.
     * @return The object of type T or an empty map if the value is null.
     */
    @SuppressWarnings("unchecked")
    private T defaultMapper(Map<String, AttributeValue> value) {
        return (T) Objects.requireNonNullElse(value, Collections.emptyMap());
    }
}
