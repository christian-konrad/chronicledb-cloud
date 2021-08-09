package de.umr.raft.raftlogreplicationdemo.services;

import de.umr.raft.raftlogreplicationdemo.models.SimpleLogEntry;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ILogPersistenceService {

    abstract CompletableFuture<SimpleLogEntry> appendEntry(String content) throws IOException;

    abstract CompletableFuture<List<SimpleLogEntry>> getLog() throws IOException;
}
