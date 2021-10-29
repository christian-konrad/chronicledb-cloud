package de.umr.raft.raftlogreplicationdemo.controllers;

import de.umr.raft.raftlogreplicationdemo.models.SimpleLogEntry;
import de.umr.raft.raftlogreplicationdemo.services.impl.SimpleLogPersistenceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

// TODO remove after testing

@RestController
@RequestMapping("/api/log")
public class SimpleLogController {

    @Autowired
    SimpleLogPersistenceService logPersistenceService;

    @PostMapping("entry")
    public SimpleLogEntry appendEntry(@RequestBody String content) throws ExecutionException, InterruptedException, IOException {
        return logPersistenceService.appendEntry(content).get();
    }

    @GetMapping("")
    public List<SimpleLogEntry> getLog() throws IOException, ExecutionException, InterruptedException {
        return logPersistenceService.getLog().get();
    }

}
