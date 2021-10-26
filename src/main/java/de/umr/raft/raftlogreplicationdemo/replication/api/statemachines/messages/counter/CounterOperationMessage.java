package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.counter.CounterOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationProto;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor(staticName = "of")
public class CounterOperationMessage implements ExecutableMessage<AtomicInteger, CounterOperationResultProto> {

    @Getter private final CounterOperationProto counterOperation;

    public static CounterOperationMessage of(ByteString bytes) throws InvalidProtocolBufferException {
        return CounterOperationMessage.of(CounterOperationProto.parseFrom(bytes));
    }

    @Override
    public ByteString getContent() {
        return counterOperation.toByteString();
    }

    @Override
    public boolean isTransactionMessage() {
        switch (counterOperation.getOperationType()) {
            case INCREMENT:
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return counterOperation.toString();
    }

    @Override
    public CompletableFuture<CounterOperationResultProto> apply(AtomicInteger counter) {
        return CounterOperationExecutor.of(counterOperation).apply(counter);
    }

    @Override
    public CompletableFuture<CounterOperationResultProto> cancel() {
        return CounterOperationExecutor.of(counterOperation).cancel();
    }

    public static class Factory {
        static CounterOperationMessage createOperationMessageOfType(CounterOperationType operationType) {
            val counterOperation = CounterOperationProto.newBuilder()
                    .setOperationType(operationType)
                    .build();

            return CounterOperationMessage.of(counterOperation);
        }

        public static CounterOperationMessage createIncrementOperationMessage() {
            return createOperationMessageOfType(CounterOperationType.INCREMENT);
        }

        public static CounterOperationMessage createGetOperationMessage() {
            return createOperationMessageOfType(CounterOperationType.GET);
        }

        public static CounterOperationMessage createNullOperationMessage() {
            return createOperationMessageOfType(CounterOperationType.NULL);
        }
    }
}
