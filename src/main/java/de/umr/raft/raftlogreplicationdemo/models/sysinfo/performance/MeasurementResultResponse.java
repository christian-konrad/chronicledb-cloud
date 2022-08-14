package de.umr.raft.raftlogreplicationdemo.models.sysinfo.performance;

import lombok.Builder;
import lombok.Getter;

@Builder
public class MeasurementResultResponse {
    @Getter private final String message;
    @Getter private final long timeElapsed;
    @Getter private final int eventCount;
    @Getter private final int batches;
    @Getter private final int threadsPerBatch;
    @Getter private final long bufferSize;
    @Getter private final boolean isBufferEnabled;
}
