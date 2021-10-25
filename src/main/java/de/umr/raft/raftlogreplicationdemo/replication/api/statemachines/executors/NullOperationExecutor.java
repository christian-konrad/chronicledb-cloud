package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.NullOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataOperationExecutor;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


public interface NullOperationExecutor<Ignorable> extends OperationExecutor<Ignorable, NullOperationResultProto> {

    // Does nothing and returns an empty (null) message
    @Override
    default CompletableFuture<NullOperationResultProto> apply(Ignorable untouched) {
        return CompletableFuture.completedFuture(NullOperationResultProto.newBuilder().setStatus(OperationResultStatus.OK).build());
    }

    @Override
    default CompletableFuture<NullOperationResultProto> cancel() {
        return CompletableFuture.completedFuture(NullOperationResultProto.newBuilder().setStatus(OperationResultStatus.CANCELLED).build());
    }
}
