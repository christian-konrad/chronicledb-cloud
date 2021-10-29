package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.metadata.MetadataOperationMessage;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.Map;
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

    @Override
    protected CounterOperationMessage createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException {
        return CounterOperationMessage.of(byteString);
    }
}
