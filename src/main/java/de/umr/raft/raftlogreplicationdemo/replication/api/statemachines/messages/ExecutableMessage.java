package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.OperationExecutionResult;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.metadata.MetadataOperationMessage;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Extension of the ratis message object that is executable.
 * On execution, a future containing a response message is returned.
 * @param <ExecutionTarget>
 */
public interface ExecutableMessage<ExecutionTarget, ExecutionResultProto extends org.apache.ratis.thirdparty.com.google.protobuf.Message> extends Message {
    Charset UTF8 = StandardCharsets.UTF_8;
    Message ERROR_MESSAGE = Message.valueOf("ERROR");

    // or executeOn ?
    CompletableFuture<ExecutionResultProto> apply(ExecutionTarget executionTarget);

    /**
     * Cancels the execution of the message's command and returns a certain status code or message
     */
    CompletableFuture<ExecutionResultProto> cancel();

    boolean isTransactionMessage();

    default boolean isQueryMessage() {
        return !isTransactionMessage();
    }

    default boolean isValid() { return true; }
}
