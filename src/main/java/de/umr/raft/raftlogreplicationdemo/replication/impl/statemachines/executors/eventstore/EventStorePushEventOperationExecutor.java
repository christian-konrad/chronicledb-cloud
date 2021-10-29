package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore.EventStoreTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class EventStorePushEventOperationExecutor implements EventStoreTransactionOperationExecutor {
    @Getter
    private final EventStoreOperationProto eventStoreOperation;

    @Override
    public CompletableFuture<EventStoreOperationResultProto> apply(EventStoreState eventStoreState) {
        val eventProto = eventStoreOperation.getEvent();

        // TODO catch errors and return success = false

        val eventStore = eventStoreState.getEventStore();
        val eventSerializer = EventSerializer.of(eventStore.getSchema());
        eventSerializer.setSerializationType(EventSerializationType.NATIVE);
        val event = eventSerializer.fromProto(eventProto);
        eventStore.pushEvent(event);

        return CompletableFuture.completedFuture(createEventStoreOperationResult());
    }

    private EventStoreOperationResultProto createEventStoreOperationResult() {
        return EventStoreOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public EventStoreOperationType getOperationType() {
        return EventStoreOperationType.PUSH_EVENT;
    }
}
