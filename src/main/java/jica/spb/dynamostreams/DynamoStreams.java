package jica.spb.dynamostreams;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.Record;

import java.util.*;

import jica.spb.dynamostreams.model.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * DynamoStreams
 * @param <T>
 */
public class DynamoStreams<T> {

    private static final Logger LOGGER = Logger.getLogger(DynamoStreams.class.getName());
    private final StreamRequest<T> streamRequest;
    private final FutureUtils futureUtils;
    private final PollConfig pollConfig;
    private StreamObserver<T> streamObserver;
    private final Map<String, StreamShard> shardsMap = new ConcurrentHashMap<>();

    /**
     * @param streamRequest StreamRequest
     */
    public DynamoStreams(StreamRequest<T> streamRequest) {
        this.streamRequest = streamRequest;
        this.futureUtils = new FutureUtils(streamRequest.getExecutor());
        this.pollConfig = streamRequest.getPollConfig();
    }

    private AmazonDynamoDBStreams streamsClient() {
        return streamRequest.getDynamoDBStreams();
    }

    /**
     * @param observer StreamObserver
     */
    public void subscribe(StreamObserver<T> observer) {
        LOGGER.log(Level.INFO, "Started DynamoDB stream subscription");
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
        LOGGER.log(Level.FINE, "Started streaming operations");
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

    /**
     * @return StreamShards
     */
    public StreamShards getShards() {
        return new StreamShards(new ArrayList<>(shardsMap.values()));
    }

    private void populateShards() {
        String lastEvaluatedShardId = null;
        int currentCount = 0;
        do {
            LOGGER.log(Level.FINER, "New Stream describe with lastEvaluatedShardId: {}", lastEvaluatedShardId);
            DescribeStreamResult streamResult = streamsClient().describeStream(
                    new DescribeStreamRequest()
                            .withStreamArn(streamRequest.getStreamARN())
                            .withExclusiveStartShardId(lastEvaluatedShardId)
            );

            newShards(streamResult.getStreamDescription().getShards());
            lastEvaluatedShardId = streamResult.getStreamDescription().getLastEvaluatedShardId();

        } while (lastEvaluatedShardId != null && ++currentCount < pollConfig.getStreamDescriptionLimitPerPoll());
    }

    private void newShards(List<Shard> shards) {
        List<StreamShard> newShards = putNewShardsInSharedMap(shards);
        LOGGER.log(Level.FINER, "Found new shards: {}", newShards);
        if (!newShards.isEmpty()) {
            emitEvent(EventType.NEW_SHARD_EVENT, newShards, null);
        }
    }

    private List<StreamShard> putNewShardsInSharedMap(List<Shard> shards) {
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
                .map(futureUtils.mapToFuture(this::getRecord))
                .collect(Collectors.toList());

        return futureUtils.allOff(futures)
                .thenApply(futureUtils.flatMapFutures(futures))
                .exceptionally(futureUtils::exceptionally)
                .join();
    }

    private List<Record> getRecord(StreamShard streamShard) {
        try {
            LOGGER.log(Level.FINEST, "Getting records for iterator: {}", streamShard.getShardIterator());

            GetRecordsResult recordsResult = streamsClient().getRecords(new GetRecordsRequest()
                    .withShardIterator(streamShard.getShardIterator()));

            streamShard.setShardIterator(recordsResult.getNextShardIterator());

            LOGGER.log(Level.FINEST, "Result record: {}", recordsResult);
            return recordsResult.getRecords();
        } catch (ExpiredIteratorException e) {
            LOGGER.log(Level.FINEST, "ShardIterator expired: {}", streamShard.getShardIterator());
            streamShard.setShardIterator(null);
            return Collections.emptyList();
        }
    }

    private void populateShardIterators() {
        var futures = shardsMap.values().stream()
                .filter(shard -> shard.getShardIterator() == null)
                .map(futureUtils.mapToFuture(this::getSharedIterator))
                .collect(Collectors.toList());

        futureUtils.allOff(futures)
                .thenApply(futureUtils.joinFutures(futures))
                .exceptionally(futureUtils::exceptionally)
                .join();
    }

    private Void getSharedIterator(StreamShard streamShard) {
        LOGGER.log(Level.FINEST, "Getting iterator for shard: {}", streamShard);

        GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest()
                .withStreamArn(streamRequest.getStreamARN())
                .withShardId(streamShard.getShard().getShardId())
                .withShardIteratorType(streamRequest.getShardIteratorType());

        GetShardIteratorResult iteratorResult = streamsClient().getShardIterator(iteratorRequest);

        LOGGER.log(Level.FINEST, "Result iterator for shard: {}", iteratorResult);

        streamShard.setShardIterator(iteratorResult.getShardIterator());
        return null;
    }

    private void removeExpiredShards() {
        LOGGER.log(Level.FINER, "Removing shards with existing shards {}", shardsMap);
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
        LOGGER.log(Level.FINEST, "Emitting event: {}", event);
        streamObserver.onChanged(event);
    }

    private boolean isEventChosen(EventType eventType) {
        return streamRequest.getEventTypes().contains(eventType);
    }

    /**
     * shutdown
     */
    public void shutdown() {
        if (pollConfig.isOwnExecutor()) {
            pollConfig.getScheduledExecutorService().shutdown();
        }
    }

}
