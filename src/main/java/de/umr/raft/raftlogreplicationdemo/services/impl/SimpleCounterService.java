package de.umr.raft.raftlogreplicationdemo.services.impl;

import de.umr.raft.raftlogreplicationdemo.services.ICounterService;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SimpleCounterService implements ICounterService {

    private final static AtomicInteger counter = new AtomicInteger();

    @Override
    public CompletableFuture increment() throws IOException {
        counter.incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Integer> getCounter() throws IOException {
        return CompletableFuture.completedFuture(counter.intValue());
    }
}
