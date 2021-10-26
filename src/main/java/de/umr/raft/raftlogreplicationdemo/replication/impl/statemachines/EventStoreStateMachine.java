package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * State machine implementation for ChronicleDB event store server application.
 */
public class EventStoreStateMachine extends ExecutableMessageStateMachine<EventStore, EventStoreOperationMessage, EventStoreOperationResultProto> {

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    protected AtomicInteger getStateObject() {
        return counter;
    }

    @Override
    protected void clearState() {
        counter.set(0);
    }

    @Override
    protected void restoreState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        counter.set(JavaUtils.cast(in.readObject()));
    }

    @Override
    protected CounterOperationMessage createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException {
        return CounterOperationMessage.of(byteString);
    }
}
