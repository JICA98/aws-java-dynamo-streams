package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Shard;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public final class StreamShard {

    private final Shard shard;

    private String shardIterator;

    public StreamShard(Shard shard) {
        this.shard = shard;
        this.shardIterator = null;
    }

}
