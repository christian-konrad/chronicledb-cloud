package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.eventstore.EventStoreOperationMessage;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Iterator;
import java.util.List;

public class EventStoreReplicationClient extends PartitionedRaftReplicationClient<EventStoreOperationMessage> {

    public String getStreamId() {
        return getPartitionId();
    }

    public static EventStoreOperationMessage createPushEventOperationMessage(Event event, EventSchema eventSchema) throws InvalidProtocolBufferException {
        return EventStoreOperationMessage.Factory.createPushEventOperationMessage(event, eventSchema);
    }

    public static EventStoreOperationMessage createPushBulkEventsOperationMessage(Iterator<Event> events, boolean ordered, EventSchema eventSchema) throws InvalidProtocolBufferException {
        return EventStoreOperationMessage.Factory.createPushBulkEventsOperationMessage(events, ordered, eventSchema);
    }

    public static EventStoreOperationMessage createGetAggregatesEventOperationMessage(Range<Long> range, List<? extends EventAggregate> list) {
        return EventStoreOperationMessage.Factory.createGetAggregatesEventOperationMessage(range, list);
    }

    public static EventStoreOperationMessage createGetKeyRangeEventOperationMessage() {
        return EventStoreOperationMessage.Factory.createGetKeyRangeEventOperationMessage();
    }

    /*
    Instant start = Instant.now();
            val client = createClientForCounterId(counterId);
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("Creating client for counter took " + timeElapsed + "ms");

            start = Instant.now();
            val result = client.sendAndExecuteOperationMessage(
                    operationMessage,
                    CounterOperationResultProto.parser());
            finish = Instant.now();
            timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("Sending message to counter client took " + timeElapsed + "ms");
     */

    // TODO query message

    @Autowired
    public EventStoreReplicationClient(RaftConfig raftConfig, String streamId) {
        super(raftConfig, streamId);
    }
}
