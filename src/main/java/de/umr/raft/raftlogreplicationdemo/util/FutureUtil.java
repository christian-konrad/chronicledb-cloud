package de.umr.raft.raftlogreplicationdemo.util;

import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.CounterReplicationClient;
import lombok.val;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class FutureUtil {

    private static final Map<String, ExecutorService> registeredThreadPools = new HashMap<>();

    public static final String GLOBAL_THREAD_POOL = "GLOBAL";

    private static ExecutorService getThreadPool(String threadPoolKey) {
        if (threadPoolKey.equals(GLOBAL_THREAD_POOL)) {
            return Executors.newCachedThreadPool();
        }
        if (!registeredThreadPools.containsKey(threadPoolKey)) {
            registeredThreadPools.put(threadPoolKey, Executors.newCachedThreadPool());
        }
        return registeredThreadPools.get(threadPoolKey);
    }

    public static <ReturnType> CompletableFuture<ReturnType> wrapInCompletableFuture(Callable<ReturnType> callable, String threadPoolKey) {
        CompletableFuture<ReturnType> completableFuture = new CompletableFuture<>();

        getThreadPool(threadPoolKey).submit(() -> {
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

    public static <ReturnType> CompletableFuture<ReturnType> wrapInCompletableFuture(Callable<ReturnType> callable) {
        return wrapInCompletableFuture(callable, GLOBAL_THREAD_POOL);
    }
}
