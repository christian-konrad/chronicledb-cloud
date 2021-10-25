package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationType;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

import java.util.concurrent.CompletableFuture;

// TODO or name StateMachineOperationExecutor?
// TODO have interfaces for TransactionOperationExecutors and QueryOperationExecutors

/**
 * Executor class that applies an operation on the executionTarget and returns a result (as a protobuf message)
 * @param <ExecutionTarget>
 * @param <ResultTypeProto>
 */
public interface OperationExecutor<ExecutionTarget, ResultTypeProto extends Message> {

    CompletableFuture<ResultTypeProto> apply(ExecutionTarget executionTarget);
    CompletableFuture<ResultTypeProto> cancel();
}
