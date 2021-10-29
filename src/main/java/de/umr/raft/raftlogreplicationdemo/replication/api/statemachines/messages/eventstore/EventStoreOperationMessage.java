package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.eventstore;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore.EventStoreOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.AggregateRequestSerializer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

// TODO may parametrize return proto type?
@RequiredArgsConstructor(staticName = "of")
public class EventStoreOperationMessage implements ExecutableMessage<EventStoreState, EventStoreOperationResultProto> {

    @Getter private final EventStoreOperationProto eventStoreOperation;

    public static EventStoreOperationMessage of(ByteString bytes) throws InvalidProtocolBufferException {
        return EventStoreOperationMessage.of(EventStoreOperationProto.parseFrom(bytes));
    }

    @Override
    public ByteString getContent() {
        return eventStoreOperation.toByteString();
    }

    // TODO may implement isValid and check message schema

    @Override
    public boolean isTransactionMessage() {
        switch (eventStoreOperation.getOperationType()) {
            case PUSH_EVENT:
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return eventStoreOperation.toString();
    }

    @Override
    public CompletableFuture<EventStoreOperationResultProto> apply(EventStoreState eventStoreState) {
        return EventStoreOperationExecutor.of(eventStoreOperation).apply(eventStoreState);
    }

    @Override
    public CompletableFuture<EventStoreOperationResultProto> cancel() {
        return EventStoreOperationExecutor.of(eventStoreOperation).cancel();
    }

    private static EventSerializer createEventSerializer(EventSchema eventSchema) {
        val eventSerializer = EventSerializer.of(eventSchema);
        eventSerializer.setSerializationType(EventSerializationType.NATIVE);
        return eventSerializer;
    }

    public static class Factory {
        public static EventStoreOperationMessage createPushEventOperationMessage(Event event, EventSchema eventSchema) throws InvalidProtocolBufferException {
            val eventSerializer = createEventSerializer(eventSchema);
            val eventProto = eventSerializer.toProto(event);

            val eventStoreOperation = EventStoreOperationProto.newBuilder()
                    .setOperationType(EventStoreOperationType.PUSH_EVENT)
                    .setEvent(eventProto)
                    .build();

            return EventStoreOperationMessage.of(eventStoreOperation);
        }

        public static EventStoreOperationMessage createGetAggregatesEventOperationMessage(Range<Long> range, List<? extends EventAggregate> list) {
            val aggregateRequestProto = AggregateRequestSerializer.toProto(range, list);

            val eventStoreOperation = EventStoreOperationProto.newBuilder()
                    .setOperationType(EventStoreOperationType.AGGREGATE)
                    .setAggregateRequest(aggregateRequestProto)
                    .build();

            return EventStoreOperationMessage.of(eventStoreOperation);
        }

        // TODO query etc

        public static EventStoreOperationMessage createNullOperationMessage() {
            val eventStoreOperation = EventStoreOperationProto.newBuilder()
                    .setOperationType(EventStoreOperationType.NULL)
                    .build();

            return EventStoreOperationMessage.of(eventStoreOperation);
        }
    }
}
