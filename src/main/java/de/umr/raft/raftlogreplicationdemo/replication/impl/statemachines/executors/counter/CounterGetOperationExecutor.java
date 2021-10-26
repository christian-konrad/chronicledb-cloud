package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.counter;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.counter.CounterQueryOperationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor(staticName = "of")
public class CounterGetOperationExecutor implements CounterQueryOperationExecutor {
    @Getter private final CounterOperationProto counterOperation;

    @Override
    public CompletableFuture<CounterOperationResultProto> apply(AtomicInteger counter) {
        val result = counter.get();
        return CompletableFuture.completedFuture(createCounterOperationResult(result));
    }

    private CounterOperationResultProto createCounterOperationResult(Integer counterValue) {
        return CounterOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setCounterValue(counterValue)
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public CounterOperationType getOperationType() {
        return CounterOperationType.GET;
    }
}
