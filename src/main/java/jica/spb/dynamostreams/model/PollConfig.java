package jica.spb.dynamostreams.model;

import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Value
public class PollConfig {

    private static final int STREAM_DESCRIPTION_LIMIT = 10;
    private static final long DEFAULT_DELAY = 10L;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    private static final Long DEFAULT_INITIAL_DELAY = 1L;

    int streamDescriptionLimitPerPoll;

    ScheduledExecutorService scheduledExecutorService;

    @Accessors(fluent = true)
    boolean usePolling;

    boolean ownExecutor;

    Long delay;

    Long initialDelay;

    TimeUnit timeUnit;

    public PollConfig(
            Integer streamDescriptionLimitPerPoll,
            ScheduledExecutorService scheduledExecutorService,
            boolean usePolling,
            Long delay,
            Long initialDelay, TimeUnit timeUnit
    ) {
        this.usePolling = usePolling;
        this.streamDescriptionLimitPerPoll = Objects.requireNonNullElse(streamDescriptionLimitPerPoll, STREAM_DESCRIPTION_LIMIT);
        this.delay = Objects.requireNonNullElse(delay, DEFAULT_DELAY);
        this.initialDelay = Objects.requireNonNullElse(initialDelay, DEFAULT_INITIAL_DELAY);
        this.timeUnit = Objects.requireNonNullElse(timeUnit, DEFAULT_TIME_UNIT);
        if (scheduledExecutorService == null) {
            this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
            ownExecutor = true;
        } else {
            ownExecutor = false;
            this.scheduledExecutorService = scheduledExecutorService;
        }
    }

    public PollConfig() {
        this(null, null, true, null, null, null);
    }

    public static PollConfigBuilder builder() {
        return new PollConfigBuilder();
    }

    public static class PollConfigBuilder {
        private Integer streamDescriptionLimitPerPoll = null;
        private ScheduledExecutorService scheduledExecutorService = null;
        private boolean usePolling = true;
        private Long delay = null;
        private TimeUnit timeUnit = null;

        public PollConfigBuilder streamDescriptionLimitPerPoll(Integer streamDescriptionLimitPerPoll) {
            this.streamDescriptionLimitPerPoll = streamDescriptionLimitPerPoll;
            return this;
        }

        public PollConfigBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        public PollConfigBuilder usePolling(boolean usePolling) {
            this.usePolling = usePolling;
            return this;
        }

        public PollConfigBuilder delay(Long delay) {
            this.delay = delay;
            return this;
        }

        public PollConfigBuilder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public PollConfig build() {
            return new PollConfig(streamDescriptionLimitPerPoll, scheduledExecutorService, usePolling, delay, null, timeUnit);
        }
    }

}
