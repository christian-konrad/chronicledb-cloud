package de.umr.raft.raftlogreplicationdemo.controllers;

import de.umr.raft.raftlogreplicationdemo.models.counter.CreateCounterRequest;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.services.impl.ReplicatedCounterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController
public class ReplicatedCounterController {

    @Autowired
    ReplicatedCounterService counterService;

    // TODO this endpoint should return all counters with ids
    @GetMapping(value = "/api/counter/replicated", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getCounters() throws IOException, ExecutionException, InterruptedException {
        return counterService.getCounters().get();
    }

    // TODO GET  /api/counter/replicated/:id
    @GetMapping(value = "/api/counter/replicated/{id}", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getCounter(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return counterService.getCounter(id).get().toString();
    }

    // TODO POST  /api/counter/replicated?partition-size=3 creates new raft group with 3 peers
    @PostMapping("/api/counter/replicated")
    @ResponseStatus(value = HttpStatus.OK)
    public RaftGroupInfo createNewCounter(@RequestBody CreateCounterRequest createCounterRequest) throws IOException, ExecutionException, InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return counterService.createNewCounter(createCounterRequest).get();
    }

    // TODO pass ID
    @PostMapping("/api/counter/replicated/{id}/increment")
    @ResponseStatus(value = HttpStatus.OK)
    public void increment(@PathVariable String id) throws IOException {
        counterService.increment(id).join();
    }
}
