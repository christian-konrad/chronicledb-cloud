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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class ListPartitionsOperationExecutor implements ClusterManagementQueryOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ListPartitionsOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    private static final String ALL_STATE_MACHINES = "ALL";

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        var request = clusterManagementOperation.getRequest().getListPartitionsRequest();

        var stateMachineClassname = request.getStatemachineClassname();

         var resultFuture = stateMachineClassname.equals(ALL_STATE_MACHINES)
                 ? clusterStateManager.listPartitions()
                 : clusterStateManager.listPartitions(request.getStatemachineClassname());

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<List<PartitionInfo>> resultFuture) {
        return resultFuture.thenApply(partitionInfos ->
                ClusterManagementOperationResultProto.newBuilder()
                        .setOperationType(getOperationType())
                        .setResponse(ClusterManagementResponseProto.newBuilder()
                                .setListPartitionsResponse(ListPartitionsResponseProto.newBuilder()
                                        .addAllPartitionInfo(partitionInfos.stream().map(PartitionInfoProtoSerializer::toProto).collect(Collectors.toList()))
                                        .build())
                                .build())
                        .setStatus(OperationResultStatus.OK)
                        .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.LIST_PARTITIONS;
    }
}
