package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore.EventStoreTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class ClearOperationExecutor implements EventStoreTransactionOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ClearOperationExecutor.class);

    @Getter
    private final EventStoreOperationProto eventStoreOperation;

    @Override
    public CompletableFuture<EventStoreOperationResultProto> apply(EventStoreState eventStoreState) {
        try {
            eventStoreState.clearStream();
        } catch (IOException e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(createUnsuccessfulOperationResult());
        }
        return CompletableFuture.completedFuture(createEventStoreOperationResult());
    }

    private EventStoreOperationResultProto createUnsuccessfulOperationResult() {
        return EventStoreOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setStatus(OperationResultStatus.ERROR)
                .build();
    }

    private EventStoreOperationResultProto createEventStoreOperationResult() {
        return EventStoreOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public EventStoreOperationType getOperationType() {
        return EventStoreOperationType.CLEAR;
    }
}
