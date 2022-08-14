package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.NullOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.OperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement.DetachPartitionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement.HeartbeatOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement.ListPartitionsOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement.RegisterPartitionOperationExecutor;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

import java.util.concurrent.CompletableFuture;

// TODO annotations and codegen to avoid long names and obsolete coding blocks;
// TODO just have @OperationExecutor annotation which automatically registers the executors

public interface ClusterManagementOperationExecutor<ResultType extends Message> extends OperationExecutor<ClusterStateManager, ResultType> {
    static ClusterManagementOperationExecutor of(ClusterManagementOperationProto clusterManagementOperation) {
        switch (clusterManagementOperation.getOperationType()) {
            case REGISTER_PARTITION:
                return RegisterPartitionOperationExecutor.of(clusterManagementOperation);
            case DETACH_PARTITION:
                return DetachPartitionOperationExecutor.of(clusterManagementOperation);
            case LIST_PARTITIONS:
                return ListPartitionsOperationExecutor.of(clusterManagementOperation);
            case HEARTBEAT:
                return HeartbeatOperationExecutor.of(clusterManagementOperation);
            case NULL:
            case UNRECOGNIZED:
            default:
                return ClusterManagementNullOperationExecutor.getInstance();
        }
    }

    ClusterManagementOperationType getOperationType();

    @SuppressWarnings("unchecked")
    default ResultType createCancellationResponse(ClusterManagementOperationType operationType) {
        return (ResultType) ClusterManagementOperationResultProto.newBuilder()
                .setOperationType(operationType)
                .setStatus(OperationResultStatus.CANCELLED)
                .build();
    }

    @Override
    public default CompletableFuture<ResultType> cancel() {
        return CompletableFuture.completedFuture(createCancellationResponse(getOperationType()));
    }

    class ClusterManagementNullOperationExecutor implements ClusterManagementOperationExecutor<NullOperationResultProto>, NullOperationExecutor<ClusterStateManager> {
        private static final ClusterManagementNullOperationExecutor INSTANCE = new ClusterManagementNullOperationExecutor();

        private ClusterManagementNullOperationExecutor() {}

        static ClusterManagementNullOperationExecutor getInstance() {
            return INSTANCE;
        }

        @Override
        public ClusterManagementOperationType getOperationType() {
            return ClusterManagementOperationType.NULL;
        }

        @Override
        public CompletableFuture<NullOperationResultProto> cancel() {
            return NullOperationExecutor.super.cancel();
        }
    }
}
