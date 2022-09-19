package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.models.sysinfo.ClusterHealth;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class GetStateMachineForRaftGroupOperationExecutor implements ClusterManagementQueryOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GetStateMachineForRaftGroupOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        var request = clusterManagementOperation.getRequest().getGetStateMachineForRaftGroupRequest();

        var resultFuture = clusterStateManager.getRaftGroupStateMachineClassname(RaftGroupId.valueOf(request.getRaftGroupId()));

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<String> resultFuture) {
        return resultFuture.thenApply(stateMachineClassname ->
                ClusterManagementOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResponse(ClusterManagementResponseProto.newBuilder()
                        .setGetStateMachineForRaftGroupResponse(GetStateMachineForRaftGroupResponseProto.newBuilder()
                                .setStateMachineClassname(stateMachineClassname)
                                .build())
                        .build())
                .setStatus(OperationResultStatus.OK)
                .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.GET_STATEMACHINE_FOR_RAFT_GROUP;
    }
}
