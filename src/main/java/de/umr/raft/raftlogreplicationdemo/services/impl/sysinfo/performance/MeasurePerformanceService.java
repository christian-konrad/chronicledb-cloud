package de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public abstract class MeasurePerformanceService {

    protected static final Logger LOG =
            LoggerFactory.getLogger(MeasurePerformanceService.class);

    @Autowired
    RaftConfig raftConfig;

    protected CompletableFuture<String> runMeasurement(CompletableFuture measurementFuture, String onDoneMessage) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            Instant start = Instant.now();
            measurementFuture.join(); // maybe measuring here is too late as starting measuring is at a stage where future already running
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            return onDoneMessage + " [" + timeElapsed + "ms]";
        });
    }

    protected CompletableFuture<String> runMeasurement(Callable<CompletableFuture> measurementCallable, String onDoneMessage) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            Instant start = Instant.now();
            measurementCallable.call().join();
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            return onDoneMessage + " [" + timeElapsed + "ms]";
        });
    }

    protected CompletableFuture<String> runMeasurement(Callable<CompletableFuture> measurementCallable, String onDoneMessage, Integer callCount) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            Instant start = Instant.now();
            measurementCallable.call().join();
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            return onDoneMessage + " [" + timeElapsed + "ms; ~" + (timeElapsed / callCount) + "ms per call]";
        });
    }
}
