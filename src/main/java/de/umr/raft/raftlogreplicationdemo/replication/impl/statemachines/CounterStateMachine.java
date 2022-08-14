package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * State machine implementation for Counter server application. This class
 * maintain a {@link AtomicInteger} object as a state and accept two commands:
 * GET and INCREMENT, GET is a ReadOnly command which will be handled by
 * {@code query} method however INCREMENT is a transactional command which
 * will be handled by {@code applyTransaction}.
 */
public class CounterStateMachine extends ExecutableMessageStateMachine<AtomicInteger, CounterOperationMessage, CounterOperationResultProto> {

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    protected AtomicInteger getStateObject() {
        return counter;
    }

    @Override
    protected Object createStateSnapshot() {
        return getStateObject();
    }

    @Override
    protected void initState() {
        // noop
    }

    @Override
    protected void clearState() {
        counter.set(0);
    }

    @Override
    protected void restoreState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        counter.set(JavaUtils.cast(in.readObject()));
    }

//    @Override
//    public CompletableFuture<?> write(LogEntryProto entry) {
//    }

    @Override
    public CompletableFuture<ByteString> read(RaftProtos.LogEntryProto entry) {
        // if log entries do not contain any data, this is called
        LOG.info("=========================");
        LOG.info("SPECIAL READ CALLED");
        return null;
    }

    @Override
    protected CounterOperationMessage createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException {
        return CounterOperationMessage.of(byteString);
    }
}
