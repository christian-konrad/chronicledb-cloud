package de.umr.raft.raftlogreplicationdemo.controllers;

import de.umr.raft.raftlogreplicationdemo.services.ICounterService;
import de.umr.raft.raftlogreplicationdemo.services.impl.SimpleCounterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/counter")
public class SimpleCounterController {

    @Autowired
    SimpleCounterService counterService;

    private final AtomicInteger counter = new AtomicInteger();

    @GetMapping(value = "", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getCounter(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return counterService.getCounter(id).get().toString();
    }

    @PostMapping("increment")
    @ResponseStatus(value = HttpStatus.OK)
    public void increment(@PathVariable String id) throws IOException {
        counterService.increment(id).join();
    }
}
