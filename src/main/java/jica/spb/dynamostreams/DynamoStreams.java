package jica.spb.dynamostreams;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.Record;

import java.util.*;

import jica.spb.dynamostreams.model.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DynamoStreams<T> {

    private final StreamRequest<T> streamRequest;
    private final FutureUtils futureUtils;
    private final List<StreamObserver> streamObservers = new CopyOnWriteArrayList<>();
    private final Map<String, StreamShard> shardsMap = new ConcurrentHashMap<>();

    public DynamoStreams(StreamRequest<T> streamRequest) {
        this.streamRequest = streamRequest;
        this.futureUtils = new FutureUtils(streamRequest.getExecutor());
    }

    private AmazonDynamoDBStreams streamsClient() {
        return streamRequest.getDynamoDBStreams();
    }

    public void buildStreamState(StreamObserver observer) {
        streamObservers.add(observer);
        removeExpiredShards();
        populateShards();
        if (shardsMap.isEmpty()) {
            return;
        }
        populateShardIterators();
        emitRecords(getRecords());
        removeExpiredShards();
    }

    public void removeObserver(StreamObserver observer) {
        streamObservers.remove(observer);
    }

    private void populateShards() {
        String lastEvaluatedShardId = null;
        do {
            DescribeStreamResult streamResult = streamsClient().describeStream(
                    new DescribeStreamRequest()
                            .withStreamArn(streamRequest.getStreamARN())
                            .withExclusiveStartShardId(lastEvaluatedShardId)
            );

            List<Shard> shards = streamResult.getStreamDescription().getShards();

            List<StreamShard> newShards = shards.stream()
                    .filter(shard -> !shardsMap.containsKey(shard.getShardId()))
                    .map(shard -> shardsMap.put(shard.getShardId(), new StreamShard(shard)))
                    .collect(Collectors.toList());

            emitEvent(EventType.NEW_SHARD_EVENT, newShards);

            lastEvaluatedShardId = streamResult.getStreamDescription().getLastEvaluatedShardId();

        } while (lastEvaluatedShardId != null);
    }

    private List<Record> getRecords() {
        var futures = shardsMap.values().stream()
                .map(mapToFuture(this::getRecord))
                .collect(Collectors.toList());

        return futureUtils.allOff(futures)
                .thenApply(flatMapFutures(futures))
                .exceptionally(this::exceptionally)
                .join();
    }

    private List<Record> getRecord(StreamShard streamShard) {
        try {
            GetRecordsResult recordsResult = streamsClient().getRecords(new GetRecordsRequest()
                    .withShardIterator(streamShard.getShardIterator()));
            return recordsResult.getRecords();
        } catch (ExpiredIteratorException e) {
            streamShard.setShardIterator(null);
            return Collections.emptyList();
        }
    }

    private void populateShardIterators() {
        var futures = shardsMap.values().stream()
                .filter(shard -> shard.getShardIterator() == null)
                .map(mapToFuture(this::getSharedIterator))
                .collect(Collectors.toList());

        futureUtils.allOff(futures)
                .thenApply(joinFutures(futures))
                .exceptionally(this::exceptionally)
                .join();
    }

    private Void getSharedIterator(StreamShard streamShard) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                .withStreamArn(streamRequest.getStreamARN())
                .withShardId(streamShard.getShard().getShardId())
                .withShardIteratorType(streamRequest.getShardIteratorType());

        GetShardIteratorResult iteratorResult =
                streamsClient().getShardIterator(getShardIteratorRequest);

        streamShard.setShardIterator(iteratorResult.getShardIterator());
        return null;
    }

    private void removeExpiredShards() {
        var expiredShards = shardsMap.values().stream()
                .filter(shard -> shard.getShardIterator() == null)
                .map(shard -> shardsMap.remove(shard.getShard().getShardId()))
                .collect(Collectors.toList());

        if (!expiredShards.isEmpty()) {
            emitEvent(EventType.OLD_SHARD_EVENT, expiredShards);
        }
    }

    private void emitRecords(List<Record> recordList) {
        recordList.stream().filter(Objects::nonNull)
                .map(this::mapToDynamoRecord)
                .forEach(this::emitRecordEvent);
    }

    private void emitRecordEvent(DynamoRecord<T> event) {
        Optional.ofNullable(
                EventType.VALUE_MAP.get(
                        event.getOriginalRecord().getEventName()
                )
        ).ifPresent(type -> emitEvent(type, event));
    }

    private DynamoRecord<T> mapToDynamoRecord(Record recordDynamo) {
        var streamRecord = recordDynamo.getDynamodb();
        var mapper = streamRequest.getMapperFn();
        var dynamoRecord = new DynamoRecord<T>(recordDynamo);
        if (mapper != null && streamRecord != null) {
            dynamoRecord.setNewImage(mapper.apply(streamRecord.getNewImage()));
            dynamoRecord.setOldImage(mapper.apply(streamRecord.getOldImage()));
            dynamoRecord.setKeyValues(mapper.apply(streamRecord.getNewImage()));
        }
        return dynamoRecord;
    }

    private <R> void emitEvent(EventType eventType, R value) {
        if (!isEventChosen(eventType)) {
            return;
        }
        streamObservers.forEach(observer ->
                observer.onChanged(new StreamEvent<R>(eventType, value)));
    }

    private boolean isEventChosen(EventType eventType) {
        return streamRequest.getEventTypes().contains(eventType);
    }

    private <R> R exceptionally(Throwable ex) {
        return futureUtils.handleException(ex, cause -> new DynamoStreamsException("Couldn't complete future", cause));
    }

    private <R> Function<Void, Stream<R>> joinFutures(List<CompletableFuture<R>> futures) {
        return v -> futures.stream().flatMap(futureUtils::stream);
    }

    private <R> Function<Void, List<R>> flatMapFutures(List<CompletableFuture<List<R>>> futures) {
        return v -> futures.stream().flatMap(futureUtils::streamList).collect(Collectors.toList());
    }

    private <S, R> Function<S, CompletableFuture<R>> mapToFuture(Function<S, R> function) {
        return shard -> futureUtils.future(function, shard);
    }

}
