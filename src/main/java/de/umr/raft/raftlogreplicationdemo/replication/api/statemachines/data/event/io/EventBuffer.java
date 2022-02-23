package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.data.event.io;

import de.umr.event.Event;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface EventBuffer {

    CompletableFuture<Long> flush() throws IOException;

    void insert(Event event) throws IOException, InterruptedException;

    void clear();
}
