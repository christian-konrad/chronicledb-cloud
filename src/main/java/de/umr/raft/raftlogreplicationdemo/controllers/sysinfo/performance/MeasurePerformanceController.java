package de.umr.raft.raftlogreplicationdemo.controllers.sysinfo.performance;

import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance.MeasureCounterPerformanceService;
import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance.MeasureEventStorePerformanceService;
import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance.MeasureMetadataPerformanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/sys-info/performance")
public class MeasurePerformanceController {

    @Autowired MeasureEventStorePerformanceService measureEventStorePerformanceService;
    @Autowired MeasureCounterPerformanceService measureCounterPerformanceService;
    @Autowired MeasureMetadataPerformanceService measureMetadataPerformanceService;

    // TODO remove crossOrigin later
    @CrossOrigin
    @GetMapping("/measure/event-store/insert-events/{count}")
    public String runInsertIntoEventStoreMeasurements(@PathVariable Integer count, @RequestParam Optional<Integer> batchSize) throws IOException, ExecutionException, InterruptedException {
        return measureEventStorePerformanceService.runInsertIntoEventStoreMeasurements(count, batchSize).get();
    }

    @GetMapping("/measure/event-store/insert-events/emitter/start")
    public String startContinuouslyInsertIntoEventStore(@RequestParam Optional<Integer> waitMillis) throws IOException, ExecutionException, InterruptedException {
        return measureEventStorePerformanceService.startContinuouslyInsertIntoEventStore(waitMillis.orElse(10)).get();
    }

    @GetMapping("/measure/event-store/insert-events/emitter/stop")
    public String stopContinuouslyInsertIntoEventStore(@RequestParam Optional<Boolean> interrupt) throws IOException, ExecutionException, InterruptedException {
        return measureEventStorePerformanceService.stopContinuouslyInsertIntoEventStore(interrupt.orElse(false)).get();
    }

    @GetMapping("/measure/event-store/get-stream-info")
    public String runGetEventStoreStreamInfoMeasurements() throws IOException, ExecutionException, InterruptedException {
        return measureEventStorePerformanceService.runGetEventStoreStreamInfoMeasurements().get();
    }

//    @GetMapping("/measure/event-store/aggregate")
//    public String runAggregateEventStoreMeasurements() throws IOException, ExecutionException, InterruptedException {
//        return measurePerformanceService.runAggregateEventStoreMeasurements().get();
//    }

    // TODO aggregate event

    // TODO remove crossOrigin later
    @CrossOrigin
    @GetMapping("/measure/counter/replicated/increment/{count}")
    public String runIncrementReplicatedCounterMeasurements(@PathVariable Integer count, @RequestParam Optional<Integer> batchSize) throws IOException, ExecutionException, InterruptedException {
        return measureCounterPerformanceService.runIncrementReplicatedCounterMeasurements(count, batchSize).get();
    }

    @GetMapping("/measure/counter/replicated/increment/sync/{count}")
    public String runIncrementReplicatedCounterSyncMeasurements(@PathVariable Integer count) throws IOException, ExecutionException, InterruptedException {
        return measureCounterPerformanceService.runIncrementReplicatedCounterSyncMeasurements(count).get();
    }

    @GetMapping("/measure/counter/replicated/read")
    public String runReadReplicatedCounterMeasurements() throws IOException, ExecutionException, InterruptedException {
        return measureCounterPerformanceService.runReadReplicatedCounterMeasurements().get();
    }

    @GetMapping("/measure/counter/standalone/increment/{count}")
    public String runIncrementStandaloneCounterMeasurements(@PathVariable Integer count) throws IOException, ExecutionException, InterruptedException {
        return measureCounterPerformanceService.runIncrementStandaloneCounterMeasurements(count).get();
    }

    @GetMapping("/measure/counter/read")
    public String runReadStandaloneCounterMeasurements() throws IOException, ExecutionException, InterruptedException {
        return measureCounterPerformanceService.runReadStandaloneCounterMeasurements().get();
    }

    @GetMapping("/measure/metadata/get")
    public String runGetMetadataMeasurements() throws IOException, ExecutionException, InterruptedException {
        return measureMetadataPerformanceService.runGetMetadataMeasurements().get();
    }

    // TODO also test non-replicated store

}
