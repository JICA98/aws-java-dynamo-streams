package jica.spb.dynamostreams.model;

import lombok.Value;

import java.util.List;

@Value
public class StreamShards {

    List<StreamShard> shardList;

}
