package jica.spb.dynamostreams;

import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

@RequiredArgsConstructor
final class FutureUtils {

    private static final Logger LOGGER = Logger.getLogger(FutureUtils.class.getName());
    public static final String ERROR_WHILE_GETTING_FUTURE_MSG = "Error occurred while getting future: {}";

    private final Executor executor;

    public <T> CompletableFuture<Void> allOff(List<CompletableFuture<T>> futures) {
        CompletableFuture<?>[] args = futures.toArray(new CompletableFuture[0]);
        return CompletableFuture.allOf(args);
    }

    public <T, R> CompletableFuture<T> future(Function<R, T> function, R request) {
        if (executor == null) {
            return CompletableFuture.supplyAsync(() -> function.apply(request));
        } else {
            return CompletableFuture.supplyAsync(() -> function.apply(request), executor);
        }
    }

    public <T> Stream<T> stream(CompletableFuture<T> future) {
        try {
            return Optional.ofNullable(future.get()).stream();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.log(Level.WARNING, ERROR_WHILE_GETTING_FUTURE_MSG, String.valueOf(e));
            Thread.currentThread().interrupt();
            return Stream.empty();
        }
    }

    public <E, T extends Collection<E>> Stream<E> streamList(CompletableFuture<T> future) {
        return stream(future).flatMap(Collection::stream);
    }

    public <T, R extends Throwable>
    T handleException(Throwable throwable, Function<Throwable, R> throwableSupplier) throws R {
        Throwable cause = Objects.requireNonNullElse(throwable.getCause(), throwable);
        throw throwableSupplier.apply(cause);
    }

}
