package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore.EventStoreTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamIndex;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class EventStorePushEventOperationExecutor implements EventStoreTransactionOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(EventStorePushEventOperationExecutor.class);

    @Getter
    private final EventStoreOperationProto eventStoreOperation;

    @Override
    public CompletableFuture<EventStoreOperationResultProto> apply(EventStoreState eventStoreState) {
        val eventProtos = eventStoreOperation.getEventsList();

        // TODO catch errors and return success = false

        val eventStore = eventStoreState.getEventStore();
        val eventSerializer = EventSerializer.of(eventStore.getSchema());
        eventSerializer.setSerializationType(EventSerializationType.NATIVE);
        if (eventProtos.size() == 0) {
            // TODO handle? What should be done here?
        } else if (eventProtos.size() == 1) {
            val event = eventSerializer.fromProto(eventProtos.get(0));
            eventStore.pushEvent(event);
        } else {
            val events = eventSerializer.fromProto(eventProtos);
            eventStore.pushEvents(events.iterator(), eventStoreOperation.getPushOptions().getIsOrdered());
        }

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
        return EventStoreOperationType.PUSH_EVENTS;
    }
}
