package jica.spb.dynamostreams.config;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * PollConfig
 * A configuration class for controlling polling behavior during DynamoDB Streams subscription.
 */
@Value
public class PollConfig {

    // Default values for configuration parameters
    private static final int STREAM_DESCRIPTION_LIMIT = 10;
    private static final long DEFAULT_DELAY = 6L;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    private static final Long DEFAULT_INITIAL_DELAY = 1L;
    private static final int CORE_POOL_SIZE = 1;

    /**
     * The maximum number of stream descriptions to retrieve per polling round.
     */
    int streamDescriptionLimitPerPoll;

    /**
     * The ScheduledExecutorService for handling polling tasks.
     */
    ScheduledExecutorService scheduledExecutorService;

    /**
     * Flag to indicate if one-time polling should be used.
     */
    @Accessors(fluent = true)
    boolean oneTimePolling;

    /**
     * Flag to indicate if the PollConfig uses its own Executor (created internally) or a custom provided Executor.
     */
    boolean ownExecutor;

    /**
     * The delay between successive polling rounds.
     */
    Long delay;

    /**
     * The initial delay before starting the first polling round.
     */
    Long initialDelay;

    /**
     * The TimeUnit for specifying time durations related to polling.
     */
    TimeUnit timeUnit;

    /**
     * PollConfig constructor.
     *
     * @param streamDescriptionLimitPerPoll The maximum number of stream descriptions to retrieve per polling round.
     * @param scheduledExecutorService      The ScheduledExecutorService for handling polling tasks.
     * @param oneTimePolling                Flag to indicate if one-time polling should be used.
     * @param delay                         The delay between successive polling rounds.
     * @param initialDelay                  The initial delay before starting the first polling round.
     * @param timeUnit                      The TimeUnit for specifying time durations related to polling.
     */

    @Builder(toBuilder = true)
    public PollConfig(
            Integer streamDescriptionLimitPerPoll,
            ScheduledExecutorService scheduledExecutorService,
            boolean oneTimePolling,
            Long delay,
            Long initialDelay, TimeUnit timeUnit
    ) {
        this.oneTimePolling = oneTimePolling;
        this.streamDescriptionLimitPerPoll = Objects.requireNonNullElse(streamDescriptionLimitPerPoll, STREAM_DESCRIPTION_LIMIT);
        this.delay = Objects.requireNonNullElse(delay, DEFAULT_DELAY);
        this.initialDelay = Objects.requireNonNullElse(initialDelay, DEFAULT_INITIAL_DELAY);
        this.timeUnit = Objects.requireNonNullElse(timeUnit, DEFAULT_TIME_UNIT);
        if (scheduledExecutorService == null) {
            this.scheduledExecutorService = Executors.newScheduledThreadPool(CORE_POOL_SIZE);
            ownExecutor = true;
        } else {
            ownExecutor = false;
            this.scheduledExecutorService = scheduledExecutorService;
        }
    }

    /**
     * Default constructor for PollConfig.
     * Creates a PollConfig with default settings for one-time polling.
     */
    PollConfig() {
        this(null, null, false, null, null, null);
    }

}
