package de.umr.raft.raftlogreplicationdemo.services;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface ICounterService {

    abstract CompletableFuture increment() throws IOException;

    abstract CompletableFuture<Integer> getCounter() throws IOException;
}
