package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.models.sysinfo.ClusterHealth;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class GetClusterHealthOperationExecutor implements ClusterManagementQueryOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GetClusterHealthOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        // var request = clusterManagementOperation.getRequest().getGetClusterHealthRequestProto();
        //if (request == null) {}

        var resultFuture = clusterStateManager.getClusterHealth();

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<ClusterHealth> resultFuture) {
        return resultFuture.thenApply(o ->
                ClusterManagementOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResponse(ClusterManagementResponseProto.newBuilder()
                        .setClusterHealthResponse(ClusterHealthResponseProto.newBuilder()
                                .setIsHealthy(o.isHealthy())
                                .addAllNodeInfo(o.getNodeHealths().stream().map(nodeHealth -> NodeHealthProto.newBuilder()
                                        .setPeerId(nodeHealth.getId())
                                        .setHeartbeat(nodeHealth.getHeartbeat())
                                        .setConnectionState(ConnectionState.valueOf(nodeHealth.getConnectionState().name()))
                                        .build()).collect(Collectors.toList()))
                                .build())
                        .build())
                .setStatus(OperationResultStatus.OK)
                .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.GET_CLUSTER_HEALTH;
    }
}
