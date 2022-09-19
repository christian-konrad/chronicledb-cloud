package de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance;

import de.umr.event.Event;
import de.umr.event.impl.SimpleEvent;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.performance.MeasurementResultResponse;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.EmbeddedChronicleEngine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedChronicleEngine;
import de.umr.raft.raftlogreplicationdemo.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

// TODO start again once cluster management works

@Service
public class MeasureEventStorePerformanceService extends MeasurePerformanceService {

    protected static final Logger LOG =
            LoggerFactory.getLogger(MeasureEventStorePerformanceService.class);

    @Autowired
    ReplicatedChronicleEngine replicatedChronicleEngine;

    @Autowired
    EmbeddedChronicleEngine embeddedChronicleEngine;

    @Autowired
    RaftConfig raftConfig;

//    Object[] getRandomEventPayload() {
//        return new Object[]{true, 42, "TEST"};  // TODO randomize
//    }

    enum SecurityType {
        EQUITY, INDEX;

        private static final List<SecurityType> VALUES =
                List.of(values());
        private static final int SIZE = VALUES.size();
        private static final Random RANDOM = new Random();

        public static SecurityType random()  {
            return VALUES.get(RANDOM.nextInt(SIZE));
        }
    }

    enum TradeSymbol {
        TSLA(SecurityType.EQUITY), DAX(SecurityType.INDEX), MXWO(SecurityType.INDEX), NDX(SecurityType.INDEX), GOOG(SecurityType.EQUITY), MSFT(SecurityType.EQUITY), BNTX(SecurityType.EQUITY);

        private SecurityType securityType;

        TradeSymbol(SecurityType securityType) {
            this.securityType = securityType;
        }

        public SecurityType getSecurityType() {
            return securityType;
        }

        private static final List<TradeSymbol> VALUES =
                List.of(values());
        private static final int SIZE = VALUES.size();
        private static final Random RANDOM = new Random();

        public static TradeSymbol random()  {
            return VALUES.get(RANDOM.nextInt(SIZE));
        }
    }

    private Random priceRandom = new Random();

    private float getRandomStockPrice() {
        return new BigDecimal(0.001 + priceRandom.nextFloat() * (99.999 - 0.001)).setScale(3, RoundingMode.HALF_UP).floatValue();
    }

    Object[] getRandomEventPayload() {
        TradeSymbol tradeSymbol = TradeSymbol.random();
        return new Object[]{
                tradeSymbol.name(),
                tradeSymbol.securityType.ordinal(),
                getRandomStockPrice()
        };
    }

    List<Event> getTestEvents(long count) {
        // TODO is inclusive range?

        long gap = 100000; // 0.1 ms between each event
        long nanosScale = 1000000;
        long nanos = System.currentTimeMillis() * nanosScale - count * gap;

        return LongStream.range(0, count).mapToObj(n -> {
            Object[] payload = getRandomEventPayload();
            return new SimpleEvent(payload, nanos + n * gap);
        }).collect(Collectors.toList());
    }

    // TODO for better comparability, serialize the test data (using proto or java serialization or json) to disk and reuse
    CompletableFuture<Void> testInsertIntoReplicatedChronicleEngine(List<Event> events) throws IOException {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (Event e : events)
                replicatedChronicleEngine.pushEvent("demo-event-store", e);
            return null;
        });
    }

    CompletableFuture<Void> testInsertIntoEmbeddedChronicleEngine(List<Event> events) throws IOException {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (Event e : events)
                embeddedChronicleEngine.pushEvent("demo_event_store", e);
            return null;
        });
    }

    CompletableFuture<Void> testInsertTimestampedEventIntoReplicatedChronicleEngine() throws IOException {
        return FutureUtil.wrapInCompletableFuture(() -> { // wrap in future so thread is not blocked
            Object[] payload = getRandomEventPayload();
            // we use a nano scale in case there is an IoT device with nano resolution
            // obviously, regular machines cannot create reliable nano timestamps
            Event event = new SimpleEvent(payload, System.currentTimeMillis() * 1000000);
            replicatedChronicleEngine.pushEvent("demo-event-store", event);
            return null;
        });
    }

    CompletableFuture<Void> testGetStreamInfoFromReplicatedChronicleEngine(int count) throws IOException {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (int i = 0; i < count; i++) {
                replicatedChronicleEngine.getStreamInfo("demo-event-store");
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

    public CompletableFuture<MeasurementResultResponse> runInsertIntoEventStoreMeasurements(Integer count, Optional<Integer> batchSize) throws IOException, ExecutionException, InterruptedException {
        // takes ~ 0.15ms for 1000 events; 1.5ms for 10.000

        // for 100'000'000 events, we never get past this...
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
                    "Batch " + (i + 1) + ": Inserted events " + threadsPerBatch + " times",
                    count
            ).join();
            LOG.info(batchResultMessage);
            resultMessage += batchResultMessage + "\n";
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();

        String totalTimeMessage = "Total time: " + timeElapsed + "ms\n";

        MeasurementResultResponse response = MeasurementResultResponse.builder()
            .message(resultMessage + totalTimeMessage)
            .timeElapsed(timeElapsed)
            .eventCount(count)
            .batches(batches)
            .threadsPerBatch(threadsPerBatch)
            .bufferSize(raftConfig.getEventStoreBufferSize())
            .isBufferEnabled(raftConfig.isEventStoreBufferEnabled())
            .build();

        return CompletableFuture.completedFuture(response);
    }

    public CompletableFuture<MeasurementResultResponse> runInsertIntoEmbeddedEventStoreMeasurements(Integer count, Optional<Integer> batchSize) throws IOException, ExecutionException, InterruptedException {
        List<Event> events = getTestEvents(count);

        int threadsPerBatch = batchSize.orElse(count);
        int batches = (int) Math.ceil(Float.valueOf(count) / threadsPerBatch);

        String resultMessage = "runInsertIntoEmbeddedEventStoreMeasurements " + count +
                " times in " + batches +
                " batches (" + threadsPerBatch + " threads per batch)\n";

        LOG.info(resultMessage);

        Instant start = Instant.now();

        for (int i = 0; i < batches; i++) {
            List<Event> sublist = events.subList(i * threadsPerBatch, (i + 1) * threadsPerBatch);
            String batchResultMessage = runMeasurement(() -> testInsertIntoEmbeddedChronicleEngine(sublist),
                    "Batch " + (i + 1) + ": Inserted events " + threadsPerBatch + " times",
                    count
            ).join();
            LOG.info(batchResultMessage);
            resultMessage += batchResultMessage + "\n";
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();

        String totalTimeMessage = "Total time: " + timeElapsed + "ms\n";

        MeasurementResultResponse response = MeasurementResultResponse.builder()
                .message(resultMessage + totalTimeMessage)
                .timeElapsed(timeElapsed)
                .eventCount(count)
                .batches(batches)
                .threadsPerBatch(threadsPerBatch)
                .bufferSize(0)
                .isBufferEnabled(false)
                //.isEmbeddedEventStore(true)
                .build();

        return CompletableFuture.completedFuture(response);
    }

    public CompletableFuture<String> runGetEventStoreStreamInfoMeasurements() throws IOException, ExecutionException, InterruptedException {
        return runMeasurement(
                testGetStreamInfoFromReplicatedChronicleEngine(1000),
                "Got stream info 1000 times"
        );
    }
}
