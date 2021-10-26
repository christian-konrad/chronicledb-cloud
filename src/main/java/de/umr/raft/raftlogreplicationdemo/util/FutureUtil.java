package de.umr.raft.raftlogreplicationdemo.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class FutureUtil {
    public static <ReturnType> CompletableFuture<ReturnType> wrapInCompletableFuture(Callable<ReturnType> callable) {
        CompletableFuture<ReturnType> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            try {
                ReturnType returnValue = callable.call();

                completableFuture.complete(returnValue);
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
            return null;
        });

        return completableFuture;
    }
}
