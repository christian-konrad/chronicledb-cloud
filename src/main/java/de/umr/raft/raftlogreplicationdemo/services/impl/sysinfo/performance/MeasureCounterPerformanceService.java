package de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//@Service
public class MeasureCounterPerformanceService extends MeasurePerformanceService {

    protected static final Logger LOG =
            LoggerFactory.getLogger(MeasureCounterPerformanceService.class);

    @Autowired
    ReplicatedCounterService counterService;

    @Autowired
    SimpleCounterService simpleCounterService;

    @Autowired
    RaftConfig raftConfig;

    private static final String COUNTER_ID = "counter";

    CompletableFuture<Void> testIncrementReplicatedCounter(int count) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (int i = 0; i < count; i++) {
                counterService.increment(COUNTER_ID).join();
            };
            return null;
        });
    }

    CompletableFuture<Void> testIncrementReplicatedCounterAsync(int count) {
        var incrementCounterFutures = new CompletableFuture[count];

        for (int i = 0; i < count; i++) {
            try {
                incrementCounterFutures[i] = counterService.increment(COUNTER_ID);
            } catch (InvalidProtocolBufferException | ExecutionException | InterruptedException e) {
                return CompletableFuture.failedFuture(e);
            }
        };

        return CompletableFuture.allOf(incrementCounterFutures);
    }

    // TODO may just need a thread pool to run those
    CompletableFuture<Void> testReadReplicatedCounter(int count) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (int i = 0; i < count; i++) {
                counterService.getCounter(COUNTER_ID).join();
            };
            return null;
        });
    }

    CompletableFuture<Void> testIncrementStandaloneCounter(int count) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (int i = 0; i < count; i++) {
                simpleCounterService.increment(COUNTER_ID).join();
            };
            return null;
        });
    }

    CompletableFuture<Void> testReadStandaloneCounter(int count) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            for (int i = 0; i < count; i++) {
                simpleCounterService.getCounter(COUNTER_ID).join();
            };
            return null;
        });
    }

    public CompletableFuture<String> runIncrementReplicatedCounterMeasurements(Integer count, Optional<Integer> batchSize) throws IOException, ExecutionException, InterruptedException {
        int threadsPerBatch = batchSize.orElse(count);
        int batches = (int) Math.ceil(Float.valueOf(count) / threadsPerBatch);

        String resultMessage = "runIncrementReplicatedCounterMeasurements " + count +
                " times in " + batches +
                " batches (" + threadsPerBatch + " threads per batch)\n";

        LOG.info(resultMessage);

        Instant start = Instant.now();

        Integer counterBefore = counterService.getCounter(COUNTER_ID).join();

        for (int i = 0; i < batches; i++) {
            String batchResultMessage = runMeasurement(() -> testIncrementReplicatedCounterAsync(threadsPerBatch),
                    "Batch " + (i + 1) + ": Incremented replicated counter " + threadsPerBatch + " times asynchronously",
                    count
            ).join();
            LOG.info(batchResultMessage);
            resultMessage += batchResultMessage + "\n";
        }

        Integer counterAfter = counterService.getCounter(COUNTER_ID).join();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();

        String totalTimeMessage = "Total time: " + timeElapsed + "ms\n";

        return CompletableFuture.completedFuture(resultMessage + totalTimeMessage + "Counter before [" + counterBefore + "] and after [" + counterAfter + "]");
    }

    public CompletableFuture<String> runIncrementReplicatedCounterSyncMeasurements(Integer count) throws IOException, ExecutionException, InterruptedException {
        Integer counterBefore = counterService.getCounter(COUNTER_ID).join();

        String resultMessage = runMeasurement(
                testIncrementReplicatedCounter(count),
                "Incremented replicated counter " + count + " times synchronously"
        ).join();

        Integer counterAfter = counterService.getCounter(COUNTER_ID).join();

        return CompletableFuture.completedFuture(resultMessage + "; counter before [" + counterBefore + "] and after [" + counterAfter + "]");
    }

    public CompletableFuture<String> runReadReplicatedCounterMeasurements() throws IOException, ExecutionException, InterruptedException {
        return runMeasurement(
                testReadReplicatedCounter(1000),
                "Read replicated counter 1000 times"
        );
    }

    public CompletableFuture<String> runIncrementStandaloneCounterMeasurements(Integer count) {
        return runMeasurement(
                testIncrementStandaloneCounter(count),
                "Incremented standalone counter " + count + " times"
        );
    }

    public CompletableFuture<String> runReadStandaloneCounterMeasurements() {
        return runMeasurement(
                testReadStandaloneCounter(1000),
                "Read standalone counter 1000 times"
        );
    }
}
