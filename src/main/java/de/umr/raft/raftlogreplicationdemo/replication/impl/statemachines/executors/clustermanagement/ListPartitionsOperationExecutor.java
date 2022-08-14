package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class ListPartitionsOperationExecutor implements ClusterManagementQueryOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ListPartitionsOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterState) {
        var request = clusterManagementOperation.getRequest().getListPartitionsRequest();

         var resultFuture = clusterState.listPartitions(
              request.getStatemachineClassname()
         );

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<List<PartitionInfo>> resultFuture) {
        return resultFuture.thenApply(partitionInfo ->
                ClusterManagementOperationResultProto.newBuilder()
                        .setOperationType(getOperationType())
                        .setStatus(OperationResultStatus.OK)
                        .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.LIST_PARTITIONS;
    }
}
