package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.ClusterHealth;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionName;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.RaftPeerProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.clustermanagement.ClusterManagementOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ApplicationLogicServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterManagementClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Wraps a ClusterState in a mutable raft server context to manipulate it
 * (e.g., create a new partition/raft group)
 */

@RequiredArgsConstructor(staticName = "of")
public class ClusterStateManager {

    @Getter private final ClusterState state;
    @Getter private final Future<RaftServer> serverFuture;

    Logger LOG = LoggerFactory.getLogger(ClusterStateManager.class);

    private RaftGroup createBalancedRaftGroup(String stateMachineClassName, String partitionName, int replicationFactor) {
        // TODO real balanced; this is just for testing

        Set<RaftPeer> potentialPeers = state.getConnectedPeers();
        // TODO useful exception
        if (potentialPeers.size() < replicationFactor) throw new RuntimeException("Not enough peers available to satisfy replication factor");

        // TODO use real load balancer!
        //List<RaftPeer> peers = potentialPeers.stream().limit(replicationFactor).collect(Collectors.toList());


        var raftGroupUUID = UUID.nameUUIDFromBytes((stateMachineClassName + ":" + partitionName).getBytes(StandardCharsets.UTF_8));
        var raftGroupId = RaftGroupId.valueOf(raftGroupUUID);

        //===

        List<ClusterState.PeerGroups> peerGroups = state.pollPeers(replicationFactor);
        List<RaftPeer> peers =
                peerGroups.stream().map(ClusterState.PeerGroups::getPeer).collect(Collectors.toList());
        RaftGroup raftGroup = RaftGroup.valueOf(raftGroupId, peers);
        peerGroups.forEach(pg -> {
            pg.getGroups().add(raftGroup);
            state.addToAvailablePeers(pg);
        });

        //===

        return RaftGroup.valueOf(raftGroupId, peers);
    }

    private RaftClient createClientForAppServerPeer(RaftPeer peer) {
        return RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(RaftGroup.valueOf(ApplicationLogicServer.BASE_GROUP_ID, peer))
                .build();
    }

//    private ClusterManagementOperationResultProto sendAndExecuteOperationMessage(ClusterManagementOperationMessage operationMessage) {
//        ClusterManagementOperationResultProto resultProto;
//
//        try {
//            resultProto = clusterManagementClient.sendAndExecuteOperationMessage(
//                    operationMessage,
//                    ClusterManagementOperationResultProto.parser());
//        } catch (ExecutionException | InterruptedException | InvalidProtocolBufferException e) {
//            e.printStackTrace();
//            // TODO custom expection
//            throw new UnsupportedOperationException();
//        }
//
//        if (!resultProto.getStatus().equals(OperationResultStatus.OK)
//                || !resultProto.getOperationType().equals(operationMessage.getClusterManagementOperation().getOperationType())) {
//            // TODO custom expection
//            throw new UnsupportedOperationException();
//        }
//
//        return resultProto;
//    }

    // hack to avoid double provisioning due to ratis timeouts
    private final Map<String, String> startedProvisioningsTemp = new HashMap<>();

    public CompletableFuture provisionPartition(String stateMachineClassName, String partitionName) throws IOException {
        LOG.info("Provisioning {}:{}", stateMachineClassName, partitionName);
        if (startedProvisioningsTemp.containsKey(stateMachineClassName) && startedProvisioningsTemp.get(stateMachineClassName).equals(partitionName)) {
            LOG.info("Already provisioning {}:{}", stateMachineClassName, partitionName);
            return CompletableFuture.completedFuture(null);
        }

        startedProvisioningsTemp.put(stateMachineClassName, partitionName);

        // provision the state machine to the peers, one by one
        // to do so, create a groupmanagement API client for the respective base raft group of log server
        // then, tell it to add the new raft group

        var partitionInfo = state.getPartition(stateMachineClassName, partitionName);

        var raftGroup = partitionInfo.getRaftGroup();

        // TODO java.lang.NullPointerException: Cannot invoke "org.apache.ratis.statemachine.StateMachine because stataMachine is null

        //int provisionedPeers = 0;
        IOException originalException = null;
        for (RaftPeer peer : raftGroup.getPeers()) {
            LOG.info("Provisioning {}:{} on {}", stateMachineClassName, partitionName, peer);
            try (RaftClient client = createClientForAppServerPeer(peer)) {
                client.getGroupManagementApi(peer.getId()).add(raftGroup);
            } catch (IOException e) {
                LOG.error("Failed to add Raft group ({}) for new StateMachine instance",
                        raftGroup.getGroupId(), e);
                originalException = e;
                break;
            }
            //provisionedPeers++;
        }

        // TODO if any expection, need to tear down again the already initialized peers
        // if (provisionedPeers < raftGroup.getPeers().size()) teardown...

        // TODO handle it properly—this causes it to retry instead of stopping!
        if (originalException != null) throw originalException;

        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<PartitionInfo> registerPartition(String stateMachineClassName, String partitionName, int replicationFactor) {
        // TODO have in each StateMachine a "createProvider(partitionName)" or "provide(partitionName)" method return a provider
        LOG.info("Registering {}:{} at {} nodes", stateMachineClassName, partitionName, replicationFactor);

        if (replicationFactor < 3) {
            // TODO useful expection
            throw new RuntimeException("A replication factor of at least 3 is required");
        }

        // TODO ensure transaction context! A partition should not be considered registered if app server has not applied all transactions
        // TODO register via applogic server

        var raftGroup = createBalancedRaftGroup(stateMachineClassName, partitionName, replicationFactor);

        var partitionInfo = state.addPartition(raftGroup, partitionName, stateMachineClassName);
        // TODO method to spawn a raftClient out of a partitionInfo


        // TODO only the leader should provision, and only once! this means:
        // - after adding partition in "REGISTERING", send a "query" request over the
        // readonly API to itself
        // this will cause to spawn the state machine instance/raft group (need own proto message for that)
        // if successful, it sends a "executable" request again to itself to update partition to "REGISTERED"
        // note that this is not transactional at all, but at least seems so for the client
/*        try {
            provisionPartitionRaftGroupOnAllPeers(partitionInfo);
            state.setPartitionRunning(stateMachineClassName, partitionName);
        } catch (Exception e) {
            // error, detach partition again
            return detachPartition(stateMachineClassName, partitionName);
        }*/

        return CompletableFuture.completedFuture(partitionInfo);
    }

    public CompletableFuture acknowledgePartitionRegistration(String stateMachineClassname, String partitionName) {
        LOG.info("Requesting acknowledging partition [{}]", partitionName);

        var partition = state.setPartitionRunning(stateMachineClassname, partitionName);

        return CompletableFuture.completedFuture(partition);
    }

    public CompletableFuture<PartitionInfo> detachPartition(String stateMachineClassname, String partitionName) {
        LOG.info("Requesting detaching partition [{}]", partitionName);

        var partition = state.detachPartition(stateMachineClassname, partitionName);

        return CompletableFuture.completedFuture(partition);
    }

    public CompletableFuture<List<PartitionInfo>> listPartitions(String stateMachineClassname) {
        LOG.info("Requesting listing partitions for [{}]", stateMachineClassname);

        var partitions = state.getPartitions(stateMachineClassname);

        return CompletableFuture.completedFuture(partitions);
    }

    public CompletableFuture<List<PartitionInfo>> listPartitions() {
        LOG.info("Requesting listing all partitions");

        var partitions = state.getPartitions();

        return CompletableFuture.completedFuture(partitions);
    }

    public CompletableFuture<String> getRaftGroupStateMachineClassname(RaftGroupId raftGroupId) {
        LOG.info("Requesting state machine class name for {}", raftGroupId);

        var stateMachineClassname = state.getRaftGroupStateMachineClassname(raftGroupId);

        LOG.info("Class name is {}", stateMachineClassname);
        return CompletableFuture.completedFuture(stateMachineClassname);
    }

    public CompletableFuture<Void> handleHeartbeat(RaftPeer peer, long heartbeat) {
        LOG.trace("Received heartbeat from [{}] at {}", peer, heartbeat);

        state.updateHeartbeat(peer, heartbeat);

        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<ClusterHealth> getClusterHealth() {
        LOG.trace("Requesting cluster health");

        var clusterHealth = state.getClusterHealth();

        return CompletableFuture.completedFuture(clusterHealth);
    }

    private RaftServer getServer() throws ExecutionException, InterruptedException {
        return serverFuture.get();
    }
}
