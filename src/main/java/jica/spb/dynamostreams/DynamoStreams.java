package jica.spb.dynamostreams;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.Record;

import java.util.*;

import jica.spb.dynamostreams.config.MapperConfig;
import jica.spb.dynamostreams.config.PollConfig;
import jica.spb.dynamostreams.config.StreamConfig;
import jica.spb.dynamostreams.model.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * DynamoStreams
 * The DynamoStreams class is designed to provide a convenient way to subscribe to and process events from an Amazon DynamoDB stream. This class handles the low-level details of interacting with the DynamoDB Streams API and allows users to define custom logic to process stream events.
 *
 * @param <T> T: The type of data being processed in the stream records. This is defined by the user when creating an instance of DynamoStreams.
 */
public class DynamoStreams<T> {

    private static final Logger LOGGER = Logger.getLogger(DynamoStreams.class.getName());
    private final Map<String, StreamShard> shardsMap = new ConcurrentHashMap<>();
    private final Sinks.Many<StreamEvent<T>> sink = Sinks.many().multicast().onBackpressureBuffer();
    private final StreamConfig<T> streamConfig;
    private final FutureUtils futureUtils;
    private final PollConfig pollConfig;
    private final MapperConfig<T> mapperConfig;

    /**
     * Description
     * The constructor initializes the DynamoStreams object with the provided StreamConfig.
     * It sets up the necessary components for handling stream events, such as PollConfig based on the provided StreamConfig.
     * Parameters
     *
     * @param streamConfig : An instance of StreamConfig that contains the necessary configuration and dependencies for setting up the DynamoDB stream subscription.
     */
    public DynamoStreams(StreamConfig<T> streamConfig) {
        this.streamConfig = streamConfig;
        this.futureUtils = new FutureUtils(streamConfig.getExecutor());
        this.pollConfig = streamConfig.getPollConfig();
        this.mapperConfig = streamConfig.getMapperConfig();
    }

    private AmazonDynamoDBStreams streamsClient() {
        return streamConfig.getDynamoDBStreams();
    }

    public Flux<StreamEvent<T>> asFlux() {
        return sink.asFlux();
    }

    /**
     * Description
     * <ul><li>This method starts the DynamoDB stream subscription and initiates the processing of stream events.
     * </li><li>Depending on the <code>PollConfig</code>, the subscription can either be performed using polling or wi.</li>
     * <li>If polling is enabled, it schedules a task to periodically fetch stream events at the specified intervals.</li>
     * <li>If polling is not enabled, it performs the streaming operations directly.</li></ul>
     *
     * @return the same instance of DynamoStreams for easy configuration
     */
    public DynamoStreams<T> initialize() {
        LOGGER.log(Level.INFO, "Started DynamoDB stream subscription");
        if (pollConfig.oneTimePolling()) {
            performStreamOperations();
        } else {
            scheduleTaskWithFixedDelay();
        }
        return this;
    }

    private void scheduleTaskWithFixedDelay() {
        ScheduledExecutorService executorService = pollConfig.getScheduledExecutorService();
        Runnable task = this::performStreamOperations;
        executorService.scheduleWithFixedDelay(
                task, pollConfig.getInitialDelay(), pollConfig.getDelay(), pollConfig.getTimeUnit()
        );
    }

    private void performStreamOperations() {
        LOGGER.log(Level.FINE, "Performing streaming operations");
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
     * Description
     * This method retrieves the current set of stream shards (if any) and returns them in a StreamShards object.
     * A StreamShards object encapsulates a list of StreamShard instances, which represent individual shards of the DynamoDB stream.
     *
     * @return StreamShards An object representing the current set of stream shards.
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
                            .withStreamArn(streamConfig.getStreamARN())
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
                .withStreamArn(streamConfig.getStreamARN())
                .withShardId(streamShard.getShard().getShardId())
                .withShardIteratorType(streamConfig.getShardIteratorType());

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
        var mapper = mapperConfig.getMapperFn();
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
        sink.tryEmitNext(event);
    }

    private boolean isEventChosen(EventType eventType) {
        return streamConfig.getEventTypes().contains(eventType);
    }

    /**
     * shutdown
     * Description
     * This method shuts down the DynamoStreams instance by stopping the scheduled executor service (if it's using its own executor).
     * It should be called when the user no longer needs to process stream events or wants to gracefully terminate the stream subscription.
     */
    public void shutdown() {
        if (pollConfig.isOwnExecutor()) {
            pollConfig.getScheduledExecutorService().shutdown();
        }
    }

}
