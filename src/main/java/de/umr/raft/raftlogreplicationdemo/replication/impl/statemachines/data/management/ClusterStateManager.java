package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.RaftPeerProto;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Wraps a ClusterState in a mutable raft server context to manipulate it
 * (e.g., create a new partition/raft group)
 */

@RequiredArgsConstructor(staticName = "of")
public class ClusterStateManager {

    @Getter private final ClusterState state;
    @Getter private final Future<RaftServer> serverFuture;

    Logger LOG = LoggerFactory.getLogger(ClusterStateManager.class);

    public CompletableFuture<PartitionInfo> registerPartition(String stateMachineClassName, String partitionName, int replicationFactor) {
        // TODO have in each StateMachine a "createProvider(partitionName)" or "provide(partitionName)" method return a provider
        LOG.info("Registering {}:{} at {} nodes", stateMachineClassName, partitionName, replicationFactor);

        // TODO ensure transaction context! A partition should not be considered registered if app server has not applied all transactions
        // TODO in ClusterState, keep book of partitions with state enum REGISTERING, REGISTERED, DETACHED
        // TODO register via applogic server
        // TODO apply to state

        //state.addPartition();

        return CompletableFuture.completedFuture(null); // TODO
    }

    public CompletableFuture<Void> detachPartition(String partitionName) {
        LOG.info("Requesting detaching partition [{}]", partitionName);
        return CompletableFuture.completedFuture(null); // TODO
    }

    public CompletableFuture<List<PartitionInfo>> listPartitions(String stateMachineName) {
        LOG.info("Requesting listing partitions for [{}]", stateMachineName);
        return CompletableFuture.completedFuture(new ArrayList<>()); // TODO
    }

    public CompletableFuture<List<PartitionInfo>> listPartitions() {
        LOG.info("Requesting listing all partitions");
        return CompletableFuture.completedFuture(new ArrayList<>()); // TODO
    }

    public CompletableFuture<Void> handleHeartbeat(RaftPeer peer, long heartbeat) {
        LOG.info("Received heartbeat from [{}] at {}", peer, heartbeat);

        state.updateHeartbeat(peer, heartbeat);

        return CompletableFuture.completedFuture(null);
    }

    private RaftServer getServer() throws ExecutionException, InterruptedException {
        return serverFuture.get();
    }
}
