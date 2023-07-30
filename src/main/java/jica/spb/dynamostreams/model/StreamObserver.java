package jica.spb.dynamostreams.model;

import java.util.UUID;

public abstract class StreamObserver <T> {

    private final UUID uuid = UUID.randomUUID();

    public abstract void onChanged(StreamEvent<T> event);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamObserver<?> that = (StreamObserver<?>) o;

        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }
}
