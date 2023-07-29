package jica.spb.dynamostreams;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.*;
import jica.spb.dynamostreams.model.EventType;
import jica.spb.dynamostreams.model.StreamEvent;
import jica.spb.dynamostreams.model.StreamObserver;
import jica.spb.dynamostreams.model.StreamRequest;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static jica.spb.dynamostreams.FutureUtils.*;

@RequiredArgsConstructor
public class DynamoStreams<T> {

    private final StreamRequest<T> streamRequest;
    private final List<StreamObserver> _streamObservers = new CopyOnWriteArrayList<>();
    private final Map<String, Shard> _shardsMap = new ConcurrentHashMap<>();
    private String lastEvaluatedShardId;

    private AmazonDynamoDBStreams streamsClient() {
        return streamRequest.getDynamoDBStreams();
    }

    public void buildStreamState() {
        _getShards();
        _getRecords();
    }

    private void _getShards() {

        DescribeStreamResult describeStreamResult = streamsClient().describeStream(
                new DescribeStreamRequest()
                        .withStreamArn(streamRequest.getStreamARN())
                        .withExclusiveStartShardId(lastEvaluatedShardId)
        );

        List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

        List<Shard> newShards = shards.stream()
                .filter(shard -> _shardsMap.containsKey(shard.getShardId()))
                .peek(shard -> _shardsMap.put(shard.getShardId(), shard))
                .collect(Collectors.toList());

        _emitEvent(EventType.NewShardsEvent, newShards);
    }

    private void _getRecords() {
        if (_shardsMap.isEmpty()) {
            return;
        }
        getShardIterators()
    }

    private List<String> getShardIterators() {

        var futures = _shardsMap.keySet().stream()
                .map(shardId -> future(this::getSharedIterator, shardId, streamRequest.getExecutorService()))
                .collect(Collectors.toList());

        return allOff(futures)
                .thenApply(v -> futures.stream()
                                .flatMap(FutureUtils::stream)
                                .collect(Collectors.toList()))
                .exceptionally(ex -> handleException(ex, cause -> new DynamoStreamsException("Couldn't complete future", cause)))
                .join();
    }

    private String getSharedIterator(String shardId) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                .withStreamArn(streamRequest.getStreamARN())
                .withShardId(shardId)
                .withShardIteratorType(streamRequest.getShardIteratorType());

        GetShardIteratorResult getShardIteratorResult =
                streamsClient().getShardIterator(getShardIteratorRequest);

        return getShardIteratorResult.getShardIterator();
    }

    private <R> void _emitEvent(EventType eventType, R value) {
        if (!_isEventChosen(eventType)) {
            return;
        }
        _streamObservers.forEach(observer ->
                observer.onChanged(new StreamEvent<R>(eventType, value)));
    }

    private boolean _isEventChosen(EventType eventType) {
        return streamRequest.getEventTypes().contains(eventType);
    }

}
