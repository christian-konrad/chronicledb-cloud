package de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance;

import de.umr.event.Event;
import de.umr.event.impl.SimpleEvent;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedChronicleEngine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.services.impl.ReplicatedCounterService;
import de.umr.raft.raftlogreplicationdemo.services.impl.SimpleCounterService;
import de.umr.raft.raftlogreplicationdemo.util.FutureUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class MeasureEventStorePerformanceService extends MeasurePerformanceService {

    protected static final Logger LOG =
            LoggerFactory.getLogger(MeasureEventStorePerformanceService.class);

    @Autowired
    ReplicatedChronicleEngine chronicleEngine;

    @Autowired
    RaftConfig raftConfig;

    Object[] getRandomEventPayload() {
        return new Object[]{true, 42, "TEST"};  // TODO randomize
    }

    List<Event> getTestEvents(int count) {
        // TODO is inclusive range?

        int rangeMax = count;
        int factor = 1000;
        long nanos = System.currentTimeMillis() * 10000000 - rangeMax * factor;

        return IntStream.range(0, rangeMax).mapToObj(n -> {
            Object[] payload = getRandomEventPayload(); // TODO randomize
            return new SimpleEvent(payload, nanos + n * factor);
        }).collect(Collectors.toList());
    }

    CompletableFuture<Void> testInsertIntoReplicatedChronicleEngine(List<Event> events) throws IOException {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (Event e : events)
                chronicleEngine.pushEvent("demo-event-store", e);
            return null;
        });
    }

    // TODO log all group and node state info and changes and visualize
    //  to understand what is happening

    CompletableFuture<Void> testInsertTimestampedEventIntoReplicatedChronicleEngine() throws IOException {
        return FutureUtil.wrapInCompletableFuture(() -> { // wrap in future so thread is not blocked
            Object[] payload = getRandomEventPayload();
            // we use a nano scale in case there is an IoT device with nano resolution
            // obviously, regular machines cannot create reliable nano timestamps
            Event event = new SimpleEvent(payload, System.currentTimeMillis() * 1000000);
            chronicleEngine.pushEvent("demo-event-store", event);
            return null;
        });
    }

    CompletableFuture<Void> testGetStreamInfoFromReplicatedChronicleEngine(int count) throws IOException {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (int i = 0; i < count; i++) {
                chronicleEngine.getStreamInfo("demo-event-store");
            };
            return null;
        });
    }

    void testInsertIntoStandaloneChronicleEngine() throws IOException {
        // TODO
    }

    public CompletableFuture<String> runInsertIntoEventStoreMeasurements() throws IOException {
        // TODO Push 1 Mio Events. Repeat 1000 times
        // TODO should create a new stream, put events, then delete stream
        List<Event> events = getTestEvents(1000);

        return runMeasurement(
                testInsertIntoReplicatedChronicleEngine(events),
                "Inserted events 1000 times"
        );
    }

    ScheduledExecutorService testEventEmitterExecutor;
    ScheduledFuture<?> testEventEmitterScheduledFuture;

    // TODO measure standalone chronicleDB
    // TODO use datasets from paper to measure
    // TODO increase ratis timeouts

    public CompletableFuture<String> startContinuouslyInsertIntoEventStore(Integer waitMillis) {
        LOG.info("Starting test event emitter");

        testEventEmitterExecutor = Executors.newScheduledThreadPool(1);

        testEventEmitterScheduledFuture = testEventEmitterExecutor.scheduleAtFixedRate(() -> {
            try {
                testInsertTimestampedEventIntoReplicatedChronicleEngine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 0, waitMillis, TimeUnit.MILLISECONDS);

        return CompletableFuture.completedFuture("Test event emitter started. You must cancel it manually with the corresponding service");
    }

    public CompletableFuture<String> stopContinuouslyInsertIntoEventStore(Boolean interrupt) {
        LOG.info("Stopping test event emitter");

        if (testEventEmitterScheduledFuture == null) {
            return CompletableFuture.completedFuture("Test event emitter not running.");
        }

        testEventEmitterScheduledFuture.cancel(interrupt);

        return CompletableFuture.completedFuture("Test event emitter stopped.");
    }

    public CompletableFuture<String> runInsertIntoEventStoreMeasurements(Integer count, Optional<Integer> batchSize) throws IOException, ExecutionException, InterruptedException {
        // takes ~ 0.15ms for 1000 events; 1.5ms for 10.000
        List<Event> events = getTestEvents(count);

        int threadsPerBatch = batchSize.orElse(count);
        int batches = (int) Math.ceil(Float.valueOf(count) / threadsPerBatch);

        String resultMessage = "runInsertIntoEventStoreMeasurements " + count +
                " times in " + batches +
                " batches (" + threadsPerBatch + " threads per batch)\n";

        LOG.info(resultMessage);

        Instant start = Instant.now();

        for (int i = 0; i < batches; i++) {
            List<Event> sublist = events.subList(i * threadsPerBatch, (i + 1) * threadsPerBatch);
            String batchResultMessage = runMeasurement(() -> testInsertIntoReplicatedChronicleEngine(sublist),
                    "Batch " + (i + 1) + ": Inserted events " + threadsPerBatch + " times asynchronously",
                    count
            ).join();
            LOG.info(batchResultMessage);
            resultMessage += batchResultMessage + "\n";
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();

        String totalTimeMessage = "Total time: " + timeElapsed + "ms\n";

        return CompletableFuture.completedFuture(resultMessage + totalTimeMessage);
    }

    public CompletableFuture<String> runGetEventStoreStreamInfoMeasurements() throws IOException, ExecutionException, InterruptedException {
        return runMeasurement(
                testGetStreamInfoFromReplicatedChronicleEngine(1000),
                "Got stream info 1000 times"
        );
    }
}
