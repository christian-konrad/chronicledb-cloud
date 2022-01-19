package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.NullOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.OperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore.EventStoreGetAggregatesOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore.EventStoreGetKeyRangeOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore.EventStorePushEventOperationExecutor;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

import java.util.concurrent.CompletableFuture;

public interface EventStoreOperationExecutor<ResultType extends Message> extends OperationExecutor<EventStoreState, ResultType> {
    static EventStoreOperationExecutor of(EventStoreOperationProto eventStoreOperation) {
        switch (eventStoreOperation.getOperationType()) {
            case PUSH_EVENTS:
                return EventStorePushEventOperationExecutor.of(eventStoreOperation);
            // case QUERY:
            //     return EventStoreQueryOperationExecutor.of(eventStoreOperation);
            case AGGREGATE:
                return EventStoreGetAggregatesOperationExecutor.of(eventStoreOperation);
            case GET_KEY_RANGE:
                return EventStoreGetKeyRangeOperationExecutor.of(eventStoreOperation);
            case NULL:
            case UNRECOGNIZED:
            default:
                return EventStoreNullOperationExecutor.getInstance();
        }
    }

    EventStoreOperationType getOperationType();

    @SuppressWarnings("unchecked")
    default ResultType createCancellationResponse(EventStoreOperationType operationType) {
        return (ResultType) EventStoreOperationResultProto.newBuilder()
                .setOperationType(operationType)
                .setStatus(OperationResultStatus.CANCELLED)
                .build();
    }

    @Override
    public default CompletableFuture<ResultType> cancel() {
        return CompletableFuture.completedFuture(createCancellationResponse(getOperationType()));
    }

    class EventStoreNullOperationExecutor implements EventStoreOperationExecutor<NullOperationResultProto>, NullOperationExecutor<EventStoreState> {
        private static final EventStoreOperationExecutor.EventStoreNullOperationExecutor INSTANCE = new EventStoreOperationExecutor.EventStoreNullOperationExecutor();

        private EventStoreNullOperationExecutor() {}

        static EventStoreOperationExecutor.EventStoreNullOperationExecutor getInstance() {
            return INSTANCE;
        }

        @Override
        public EventStoreOperationType getOperationType() {
            return EventStoreOperationType.NULL;
        }

        @Override
        public CompletableFuture<NullOperationResultProto> cancel() {
            return NullOperationExecutor.super.cancel();
        }
    }
}
