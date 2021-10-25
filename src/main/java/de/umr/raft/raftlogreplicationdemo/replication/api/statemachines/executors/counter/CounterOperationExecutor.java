package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.counter;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.OperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.NullOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.counter.CounterGetOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.counter.CounterIncrementOperationExecutor;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public interface CounterOperationExecutor<ResultType extends Message> extends OperationExecutor<AtomicInteger, ResultType> {
    static CounterOperationExecutor of(CounterOperationProto counterOperation) {
        switch (counterOperation.getOperationType()) {
            case INCREMENT:
                return CounterIncrementOperationExecutor.of(counterOperation);
            case GET:
                return CounterGetOperationExecutor.of(counterOperation);
            case NULL:
            case UNRECOGNIZED:
            default:
                return CounterNullOperationExecutor.getInstance();
        }
    }

    CounterOperationType getOperationType();

    @SuppressWarnings("unchecked")
    default ResultType createCancellationResponse(CounterOperationType operationType) {
        return (ResultType) CounterOperationResultProto.newBuilder()
                .setOperationType(operationType)
                .setStatus(OperationResultStatus.CANCELLED)
                .build();
    }

    @Override
    public default CompletableFuture<ResultType> cancel() {
        return CompletableFuture.completedFuture(createCancellationResponse(getOperationType()));
    }

    class CounterNullOperationExecutor implements CounterOperationExecutor<NullOperationResultProto>, NullOperationExecutor<AtomicInteger> {
        private static final CounterNullOperationExecutor INSTANCE = new CounterNullOperationExecutor();

        private CounterNullOperationExecutor() {}

        static CounterNullOperationExecutor getInstance() {
            return INSTANCE;
        }

        @Override
        public CounterOperationType getOperationType() {
            return CounterOperationType.NULL;
        }

        @Override
        public CompletableFuture<NullOperationResultProto> cancel() {
            return NullOperationExecutor.super.cancel();
        }
    }
}
