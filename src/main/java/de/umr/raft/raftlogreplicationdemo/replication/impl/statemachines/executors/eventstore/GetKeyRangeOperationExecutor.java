package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore.EventStoreQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.AggregateRequestSerializer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class GetKeyRangeOperationExecutor implements EventStoreQueryOperationExecutor {
    @Getter
    private final EventStoreOperationProto eventStoreOperation;

    @Override
    public CompletableFuture<EventStoreOperationResultProto> apply(EventStoreState eventStoreState) {
        val eventStore = eventStoreState.getEventStore();

        val keyRange = eventStore.getKeyRange();

        // TODO check if it is properly received here

        return CompletableFuture.completedFuture(createEventStoreOperationResult(keyRange));
    }

    private EventStoreOperationResultProto createEventStoreOperationResult(Optional<Range<Long>> keyRange) {
        val builder = EventStoreOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setIsNullResult(keyRange.isEmpty())
                .setStatus(OperationResultStatus.OK);

        if (keyRange.isPresent()) {
            val keyRangeProto = AggregateRequestSerializer.toProto(keyRange.get());
            builder.setKeyRangeResult(keyRangeProto);
        }

        return builder.build();
    }

    @Override
    public EventStoreOperationType getOperationType() {
        return EventStoreOperationType.GET_KEY_RANGE;
    }
}
