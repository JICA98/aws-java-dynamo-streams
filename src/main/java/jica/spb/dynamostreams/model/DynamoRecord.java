package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Record;
import lombok.Builder;
import lombok.Value;

/**
 * DynamoRecord
 * The DynamoRecord class represents a processed record from an Amazon DynamoDB stream. It contains the original Record object along with the old image, new image, and key values extracted from the stream record. The generic type T represents the type of data contained in the old image, new image, and key values.
 *
 * @param <T> The type of data contained in the record images.
 */
@Value
@Builder
public class DynamoRecord<T> {

    /**
     * The original DynamoDB Record.
     */
    Record originalRecord;

    /**
     * The old image of the record.
     */
    T oldImage;

    /**
     * The new image of the record.
     */
    T newImage;

    /**
     * The key values of the record.
     */
    T keyValues;
}
