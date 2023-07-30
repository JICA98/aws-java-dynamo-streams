package jica.spb.dynamostreams.model;

import lombok.Value;

import java.util.List;

/**
 * StreamShards
 * A class representing a list of StreamShard objects in Amazon DynamoDB Streams.
 */
@Value
public class StreamShards {

    /**
     * The list of StreamShard objects.
     */
    List<StreamShard> shardList;

    /**
     * StreamShards constructor.
     *
     * @param shardList The list of StreamShard objects.
     */
    public StreamShards(List<StreamShard> shardList) {
        this.shardList = shardList;
    }
}
