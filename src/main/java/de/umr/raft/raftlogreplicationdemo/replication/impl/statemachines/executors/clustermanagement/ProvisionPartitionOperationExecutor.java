package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.serialization.PartitionInfoProtoSerializer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class ProvisionPartitionOperationExecutor implements ClusterManagementQueryOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ProvisionPartitionOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        var request = clusterManagementOperation.getRequest().getInstantiatePartitionRequest();

        CompletableFuture resultFuture;

        try {
            LOG.info("Provisioning started");
            resultFuture = clusterStateManager.provisionPartition(
                    request.getStatemachineClassname(),
                    request.getPartitionName()
            );
            LOG.info("Provisioning done");
        } catch (IOException e) {
            LOG.info("Provisioning canceled");
            return cancel();
        }

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<PartitionInfo> resultFuture) {
        // TODO do we need response?
        return resultFuture.thenApply(partitionInfo ->
                ClusterManagementOperationResultProto.newBuilder()
                        .setOperationType(getOperationType())
                        .setStatus(OperationResultStatus.OK)
                        .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.INSTANTIATE_PARTITION;
    }
}
