package jica.spb.dynamostreams.model;

import com.amazonaws.services.dynamodbv2.model.Shard;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public final class StreamShard {

    private final Shard shard;

    private String shardIterator;

    public StreamShard(Shard shard) {
        this.shard = shard;
        this.shardIterator = null;
    }

}
