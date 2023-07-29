package jica.spb.dynamostreams;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class FutureUtils {


    public static <T> CompletableFuture<Void> allOff(List<CompletableFuture<T>> futures) {
        CompletableFuture<?>[] args = futures.toArray(new CompletableFuture[0]);
        return CompletableFuture.allOf(args);
    }

    public static <T, R> CompletableFuture<T> future(Function<R, T> function, R request, Executor executor) {
        if (executor == null) {
            return CompletableFuture.supplyAsync(() -> function.apply(request));
        } else {
            return CompletableFuture.supplyAsync(() -> function.apply(request), executor);
        }
    }

    public static <T> Stream<T> stream(CompletableFuture<T> future) {
        try {
            return Optional.ofNullable(future.get()).stream();
        } catch (InterruptedException | ExecutionException e) {
            return Stream.empty();
        }
    }

    public static <T, R extends Throwable>
    T handleException(Throwable throwable, Function<Throwable, R> throwableSupplier) throws R {
        Throwable cause = Objects.requireNonNullElse(throwable.getCause(), throwable);
        throw throwableSupplier.apply(cause);
    }

}
