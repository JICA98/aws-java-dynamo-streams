package jica.spb.dynamostreams;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.Record;

import java.util.*;

import jica.spb.dynamostreams.model.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DynamoStreams<T> {

    private final StreamRequest<T> streamRequest;
    private final FutureUtils futureUtils;
    private final PollConfig pollConfig;
    private StreamObserver<T> streamObserver;
    private final Map<String, StreamShard> shardsMap = new ConcurrentHashMap<>();

    public DynamoStreams(StreamRequest<T> streamRequest) {
        this.streamRequest = streamRequest;
        this.futureUtils = new FutureUtils(streamRequest.getExecutor());
        this.pollConfig = streamRequest.getPollConfig();
    }

    private AmazonDynamoDBStreams streamsClient() {
        return streamRequest.getDynamoDBStreams();
    }

    public void subscribeToStream(StreamObserver<T> observer) {
        if (streamRequest.getPollConfig().usePolling()) {
            scheduleTaskWithFixedDelay(observer);
        } else {
            performStreamingOperations(observer);
        }
    }

    private void scheduleTaskWithFixedDelay(StreamObserver<T> observer) {
        ScheduledExecutorService executorService = pollConfig.getScheduledExecutorService();
        Runnable task = () -> performStreamingOperations(observer);
        executorService.scheduleWithFixedDelay(
                task, pollConfig.getInitialDelay(), pollConfig.getDelay(), pollConfig.getTimeUnit()
        );
    }

    private void performStreamingOperations(StreamObserver<T> observer) {
        streamObserver = observer;
        removeExpiredShards();
        populateShards();
        if (shardsMap.isEmpty()) {
            return;
        }
        populateShardIterators();
        emitRecords(getRecords());
        removeExpiredShards();
    }

    public StreamShards getShards() {
        return new StreamShards(new ArrayList<>(shardsMap.values()));
    }

    private void populateShards() {
        String lastEvaluatedShardId = null;
        int currentCount = 0;
        do {
            DescribeStreamResult streamResult = streamsClient().describeStream(
                    new DescribeStreamRequest()
                            .withStreamArn(streamRequest.getStreamARN())
                            .withExclusiveStartShardId(lastEvaluatedShardId)
            );

            List<Shard> shards = streamResult.getStreamDescription().getShards();
            List<StreamShard> newShards = mapToNewShards(shards);
            emitEvent(EventType.NEW_SHARD_EVENT, newShards, null);
            lastEvaluatedShardId = streamResult.getStreamDescription().getLastEvaluatedShardId();

        } while (lastEvaluatedShardId != null && ++currentCount < pollConfig.getStreamDescriptionLimitPerPoll());
    }

    private List<StreamShard> mapToNewShards(List<Shard> shards) {
        return shards.stream()
                .filter(shard -> !shardsMap.containsKey(shard.getShardId()))
                .map(shard -> {
                    StreamShard streamShard = new StreamShard(shard);
                    shardsMap.put(shard.getShardId(), streamShard);
                    return streamShard;
                })
                .collect(Collectors.toList());
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

            streamShard.setShardIterator(recordsResult.getNextShardIterator());

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
        GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest()
                .withStreamArn(streamRequest.getStreamARN())
                .withShardId(streamShard.getShard().getShardId())
                .withShardIteratorType(streamRequest.getShardIteratorType());

        GetShardIteratorResult iteratorResult =
                streamsClient().getShardIterator(iteratorRequest);

        streamShard.setShardIterator(iteratorResult.getShardIterator());
        return null;
    }

    private void removeExpiredShards() {
        var expiredShards = shardsMap.values().stream()
                .filter(shard -> shard.getShardIterator() == null)
                .map(shard -> shardsMap.remove(shard.getShard().getShardId()))
                .collect(Collectors.toList());

        if (!expiredShards.isEmpty()) {
            emitEvent(EventType.OLD_SHARD_EVENT, expiredShards, null);
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
        ).ifPresent(type -> emitEvent(type, new ArrayList<>(shardsMap.values()), event));
    }

    private DynamoRecord<T> mapToDynamoRecord(Record recordDynamo) {
        var streamRecord = recordDynamo.getDynamodb();
        var mapper = streamRequest.getMapperFn();
        return DynamoRecord.<T>builder()
                .originalRecord(recordDynamo)
                .newImage(mapper.apply(streamRecord.getNewImage()))
                .oldImage(mapper.apply(streamRecord.getOldImage()))
                .keyValues(mapper.apply(streamRecord.getNewImage()))
                .build();
    }

    private void emitEvent(EventType eventType, List<StreamShard> streamShards, DynamoRecord<T> dynamoRecord) {
        if (!isEventChosen(eventType)) {
            return;
        }
        var event = new StreamEvent<>(eventType, dynamoRecord, new StreamShards(streamShards));
        streamObserver.onChanged(event);
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

    public void shutdown() {
        if (pollConfig.isOwnExecutor()) {
            pollConfig.getScheduledExecutorService().shutdown();
        }
    }

}
