package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.counter;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.OperationExecutionResult;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.TransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.counter.CounterTransactionOperationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor(staticName = "of")
public class CounterIncrementOperationExecutor implements CounterTransactionOperationExecutor {
    @Getter private final CounterOperationProto counterOperation;

    public CompletableFuture<CounterOperationResultProto> apply(AtomicInteger counter) {
        val result = counter.incrementAndGet();
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
        return CounterOperationType.INCREMENT;
    }
}
