package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.chronicledb.event.store.EventStore;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.eventstore.EventStoreOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Path;

/**
 * State machine implementation for ChronicleDB event store server application.
 */

public class EventStoreStateMachine extends ExecutableMessageStateMachine<EventStoreState, EventStoreOperationMessage, EventStoreOperationResultProto> {

    // TODO May marry it with RaftLog as it may be the same or use log compaction
    private EventStoreState eventStoreState = EventStoreState.createUninitializedState();

    private String getStreamName() {
        // TODO should ask managementServer for real name, not UUID
        return getGroupId().toString().replace("-", "_");
    }

    // TO kill log: return getServer().get().getDivision().getRaftLog().purge()

    private Path getBasePath() {
        return storageDir.getRoot().toPath();
    }

    @Override
    protected EventStoreState getStateObject() {
        return eventStoreState;
    }

    @Override
    protected Object createStateSnapshot() {
        return eventStoreState.createSnapshot();
    }

    @Override
    protected void initState() throws IOException {
        eventStoreState.initState(getBasePath(), getStreamName());
    }

    @Override
    protected void clearState() {
        eventStoreState.clear();
    }

    @Override
    protected void restoreState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        eventStoreState.loadFrom(in);
    }

    @Override
    protected EventStoreOperationMessage createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException {
        return EventStoreOperationMessage.of(byteString);
    }

    // TODO transport events by protobuf
}
