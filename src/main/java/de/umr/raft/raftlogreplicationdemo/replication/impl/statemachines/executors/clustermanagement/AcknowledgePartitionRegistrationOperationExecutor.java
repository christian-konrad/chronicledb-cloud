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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class AcknowledgePartitionRegistrationOperationExecutor implements ClusterManagementTransactionOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgePartitionRegistrationOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        LOG.info("Execute the acknowledging the provisioning of partitions");

        var request = clusterManagementOperation.getRequest().getAcknowledgePartitionRegistrationRequest();

        var resultFuture = clusterStateManager.acknowledgePartitionRegistration(
                request.getStatemachineClassname(),
                request.getPartitionName()
        );

        LOG.info("Done executing the acknowledging the provisioning of partitions");

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<PartitionInfo> resultFuture) {
        return resultFuture.thenApply(partitionInfo ->
                ClusterManagementOperationResultProto.newBuilder()
                        .setOperationType(getOperationType())
                        .setStatus(OperationResultStatus.OK)
                        .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.ACKNOWLEDGE_PARTITION_REGISTRATION;
    }
}
