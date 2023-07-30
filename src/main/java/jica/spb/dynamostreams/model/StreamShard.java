package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Shard;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * StreamShard
 * A class representing a shard in Amazon DynamoDB Streams.
 */
@Data
@Builder
@AllArgsConstructor
public final class StreamShard {

    /**
     * The Shard object representing the shard in DynamoDB Streams.
     */
    private final Shard shard;

    /**
     * The shard iterator associated with the StreamShard.
     */
    private String shardIterator;

    /**
     * StreamShard constructor.
     *
     * @param shard The Shard object representing the shard in DynamoDB Streams.
     */
    public StreamShard(Shard shard) {
        this.shard = shard;
        this.shardIterator = null;
    }
}
