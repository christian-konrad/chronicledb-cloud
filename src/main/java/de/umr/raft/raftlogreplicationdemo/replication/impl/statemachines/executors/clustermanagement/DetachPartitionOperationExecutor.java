package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RequiredArgsConstructor(staticName = "of")
public class DetachPartitionOperationExecutor implements ClusterManagementTransactionOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DetachPartitionOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        var request = clusterManagementOperation.getRequest().getDetachPartitionRequest();

         var resultFuture = clusterStateManager.detachPartition(
                 request.getStatemachineClassname(),
                 request.getPartitionName()
         );

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<PartitionInfo> resultFuture) {
        return resultFuture.thenApply(o ->
                ClusterManagementOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setStatus(OperationResultStatus.OK)
                .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.DETACH_PARTITION;
    }
}
