package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Record;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DynamoRecord<T> {

    Record originalRecord;

    T oldImage;

    T newImage;

    T keyValues;

}
