package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementTransactionOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class HeartbeatOperationExecutor implements ClusterManagementTransactionOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatOperationExecutor.class);

    @Getter
    private final ClusterManagementOperationProto clusterManagementOperation;

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        var request = clusterManagementOperation.getRequest().getHeartbeatRequestProto();

        var peerProto = request.getPeer();

        var resultFuture = clusterStateManager.handleHeartbeat(
                RaftPeer.newBuilder()
                     .setAddress(peerProto.getAddress())
                     .setId(peerProto.getId())
                     .build(),
             System.currentTimeMillis()
        );

        return createClusterManagementOperationResult(resultFuture);
    }

    private CompletableFuture<ClusterManagementOperationResultProto> createClusterManagementOperationResult(CompletableFuture<Void> resultFuture) {
        return resultFuture.thenApply(o ->
                ClusterManagementOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setStatus(OperationResultStatus.OK)
                .build());
    }

    @Override
    public ClusterManagementOperationType getOperationType() {
        return ClusterManagementOperationType.HEARTBEAT;
    }
}
