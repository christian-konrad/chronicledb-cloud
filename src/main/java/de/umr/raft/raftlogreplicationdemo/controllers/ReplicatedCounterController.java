package de.umr.raft.raftlogreplicationdemo.controllers;

import de.umr.raft.raftlogreplicationdemo.services.impl.ReplicatedCounterService;
import de.umr.raft.raftlogreplicationdemo.services.impl.SimpleCounterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@RestController
public class ReplicatedCounterController {

    @Autowired
    ReplicatedCounterService counterService;

    @GetMapping(value = "/api/counter/replicated", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getCounter() throws IOException, ExecutionException, InterruptedException {
        return counterService.getCounter().get().toString();
    }

    @PostMapping("/api/counter/replicated/increment")
    @ResponseStatus(value = HttpStatus.OK)
    public void increment() throws IOException {
        counterService.increment().join();
    }
}
