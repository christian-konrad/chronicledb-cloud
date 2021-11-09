package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.eventstore.EventStoreOperationMessage;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class EventStoreReplicationClient extends PartitionedRaftReplicationClient<EventStoreOperationMessage> {

    public String getStreamId() {
        return getPartitionId();
    }

    public static EventStoreOperationMessage createPushEventOperationMessage(Event event, EventSchema eventSchema) throws InvalidProtocolBufferException {
        return EventStoreOperationMessage.Factory.createPushEventOperationMessage(event, eventSchema);
    }

    public static EventStoreOperationMessage createGetAggregatesEventOperationMessage(Range<Long> range, List<? extends EventAggregate> list) {
        return EventStoreOperationMessage.Factory.createGetAggregatesEventOperationMessage(range, list);
    }

    public static EventStoreOperationMessage createGetKeyRangeEventOperationMessage() {
        return EventStoreOperationMessage.Factory.createGetKeyRangeEventOperationMessage();
    }

    // TODO query message

    @Autowired
    public EventStoreReplicationClient(RaftConfig raftConfig, String streamId) {
        super(raftConfig, streamId);
    }
}
