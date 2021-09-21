package de.umr.raft.raftlogreplicationdemo.services;

import de.umr.raft.raftlogreplicationdemo.models.counter.CreateCounterRequest;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface ICounterService {

    abstract CompletableFuture getCounters() throws IOException, ExecutionException, InterruptedException;

    abstract CompletableFuture increment(String counterId) throws IOException;

    abstract CompletableFuture<Integer> getCounter(String counterId) throws IOException;

    abstract CompletableFuture createNewCounter(CreateCounterRequest createCounterRequest) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ExecutionException, InterruptedException;
}
