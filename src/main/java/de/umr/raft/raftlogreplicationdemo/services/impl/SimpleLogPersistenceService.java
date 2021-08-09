package de.umr.raft.raftlogreplicationdemo.services.impl;

import de.umr.raft.raftlogreplicationdemo.models.SimpleLogEntry;
import de.umr.raft.raftlogreplicationdemo.services.ILogPersistenceService;
import lombok.val;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Stores the log as a local file
 */
@Service
public class SimpleLogPersistenceService implements ILogPersistenceService {

    // TODO should instantiate file path with Properties file
    // TODO should have server id in file path for testing purposes
    private static final String LOG_PATH = "file:///C:/temp/raft-test";
    private static final String LOG_FILE_NAME = "log-storage.txt";

    private static void persistEntry(SimpleLogEntry logEntry) throws IOException {
        // TODO should have instantiable class SimpleLogWriter or else
        val path = Paths.get(URI.create(LOG_PATH));

        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }

        Files.write(
                Paths.get(URI.create(LOG_PATH + "/" + LOG_FILE_NAME)),
                logEntry.getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // TODO stuff to store the log entries
    @Override
    public CompletableFuture<SimpleLogEntry> appendEntry(String content) throws IOException {
        val logEntry = SimpleLogEntry.of(System.currentTimeMillis(), content);
        persistEntry(logEntry);
        return CompletableFuture.completedFuture(logEntry);
    }

    @Override
    public CompletableFuture<List<SimpleLogEntry>> getLog() throws IOException {
        val logLines = Files.readAllLines(Paths.get(URI.create(LOG_PATH + "/" + LOG_FILE_NAME)));
        val log = logLines.stream().map(logLine -> SimpleLogEntry.ofLogLine(logLine)).collect(Collectors.toList()); // TODO include timestamp here
        return CompletableFuture.completedFuture(log);
    }
}
