package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.chronicledb.event.store.EventStore;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.eventstore.EventStoreOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamIndex;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.statemachine.TransactionContext;
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
        // TODO currently we never take snapshots
        // TODO can not simply re-open the event store from file as it would reappend all entries again on startup
        // TODO how to differentiate between snapshotted and unsnapshotted state (= event store)?
        // TODO must untie outer in-memory flank (= replayable by raft) and persisted stuff (= snapshot)
        // TODO log replay will then only build up the right flank again but NEVER the persisted store
        // TODO we can also implement custom StateMachineStorage that will always refer to the event store index
        // TODO then we still need to trigger snapshotting after each insert after something is persisted in event store
        // to have reference on last applied term and index
        // ask how to know when something is persisted so we know when to store last applied term and index;
        // how to control the write-ahead buffer (as it is replaced by raft log)
        // TODO snapshot must contain last term and index (so we can ignore previous raft log entries) -> everything after it is the flank
        // and snapshot must contain the event store (or reference on it)
        // TODO check if OOO makes problems
        // TODO committing (Applying) an entry should not mean writing it to disc!
    }

    // TODO ore just a "closeState"?
    @Override
    protected void clearState() {
        eventStoreState.clear();
    }

    @Override
    protected void restoreState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        eventStoreState.loadFrom(in);
    }

//    @Override
//    public boolean getShouldLogTransactionRuntime() {
//        return true;
//    }

    @Override
    protected EventStoreOperationMessage createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException {
        return EventStoreOperationMessage.of(byteString);
    }

    @Override
    public void beforeApplyTransaction(TransactionContext trx) {
        super.beforeApplyTransaction(trx);
        if (LOG.isDebugEnabled()) {
            final RaftProtos.LogEntryProto entry = trx.getLogEntry();
            LOG.debug("Executing message " + entry.getTerm() + ":" + entry.getIndex());
        }
    }

    // TODO transport events by protobuf
}
