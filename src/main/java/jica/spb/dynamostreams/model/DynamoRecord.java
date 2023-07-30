package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Record;
import lombok.Data;

@Data
public class DynamoRecord<T> {

    private final Record originalRecord;

    private T oldImage;

    private T newImage;

    private T keyValues;

}
