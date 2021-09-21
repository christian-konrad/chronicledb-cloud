package de.umr.raft.raftlogreplicationdemo.services.impl;

import de.umr.raft.raftlogreplicationdemo.models.counter.CreateCounterRequest;
import de.umr.raft.raftlogreplicationdemo.services.ICounterService;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SimpleCounterService implements ICounterService {

    private final static AtomicInteger counter = new AtomicInteger();

    // TODO somehow respect IDs by having a map of counters

    @Override
    public CompletableFuture getCounters() throws IOException {
        return null;
    }

    @Override
    public CompletableFuture increment(String counterId) throws IOException {
        counter.incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Integer> getCounter(String counterId) throws IOException {
        return CompletableFuture.completedFuture(counter.intValue());
    }

    @Override
    public CompletableFuture createNewCounter(CreateCounterRequest createCounterRequest) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ExecutionException, InterruptedException {
        return null;
    }
}
