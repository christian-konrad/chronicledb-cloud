package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.serialization.PartitionInfoProtoSerializer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class RegisterPartitionOperationExecutor implements ClusterManagementTransactionOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(RegisterPartitionOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        var request = clusterManagementOperation.getRequest().getRegisterPartitionRequest();

         var resultFuture = clusterStateManager.registerPartition(
              request.getStatemachineClassname(),
              request.getPartitionName(),
              request.getReplicationFactor()
         );

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<PartitionInfo> resultFuture) {
        // TODO wrap in helper functions for deserialization
        return resultFuture.thenApply(partitionInfo ->
                ClusterManagementOperationResultProto.newBuilder()
                        .setOperationType(getOperationType())
                        .setResponse(ClusterManagementResponseProto.newBuilder()
                                .setRegisterPartitionResponse(RegisterPartitionResponseProto.newBuilder()
                                        .setPartitionInfo(PartitionInfoProtoSerializer.toProto(partitionInfo))
                                        .build())
                                .build())
                        .setStatus(OperationResultStatus.OK)
                        .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.REGISTER_PARTITION;
    }
}
