package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.ClusterHealth;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.NodeHealth;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionName;
import de.umr.raft.raftlogreplicationdemo.replication.api.PeerState;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.data.StateMachineState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import lombok.Getter;
import lombok.val;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Part;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterState implements StateMachineState {

    private final RaftConfig raftConfig;
    // used to expose state into local server cache
    private final ClusterManager clusterManager;

    @Getter
    private StateMachineState.Phase phase;

    // List of the currently known peers, built upon heartbeats
    private final Set<RaftPeer> peers = new HashSet<>();

    // And their health states
    private final PeerStateProvider peerStates;

    // all registered raft groups
    //private final Set<StateMachineProvider> stateMachineProviders = new HashSet<>();
    private final PartitionProvider partitions;
    private final Set<PartitionName> partitionNames = new HashSet<>();

    // mapping of the group ids to respective state machine class names
    private final Map<RaftGroupId, String> stateMachineGroupMapping = new HashMap<>();

    static class PeerGroups implements Comparable {
        private RaftPeer peer;
        private Set<RaftGroup> groups = new HashSet<>();

        PeerGroups(RaftPeer peer) {
            this.peer = peer;
        }

        public Set<RaftGroup> getGroups () {
            return groups;
        }

        public RaftPeer getPeer() {
            return peer;
        }

        @Override
        public int compareTo(Object o) {
            return groups.size() - ((PeerGroups) o).groups.size();
        }
    }

    // balanced list of peers and their registered groups
    private PriorityBlockingQueue<PeerGroups> avail = new PriorityBlockingQueue<>();

    public List<PeerGroups> pollPeers(int replicationFactor) {
        List<PeerGroups> peerGroups =
            IntStream.range(0, replicationFactor).mapToObj(i -> avail.poll()).collect(Collectors.toList());
        return peerGroups;
    }

    public void addToAvailablePeers(PeerGroups peerGroups) {
        avail.add(peerGroups);
        LOG.info("Available peer group mappings: {}", avail);
    }
    // TODO lastStartedAt

    protected static final Logger LOG =
            LoggerFactory.getLogger(ClusterState.class);

    private final Consumer<RaftPeer> handlePeerDisconnected = peer -> {
        // TODO also on disconnected, remove peer from it's partitions via adminAPI of the corresponding groups (therefore, need clients)
        // TODO also remove from loadBalancer list
        // TODO if it comes back again, re-add it
        peers.remove(peer);

        final List<PeerGroups> peerGroupsToRemove = new ArrayList<>();
        // remove peer groups from avail.
        avail.forEach(peerGroup -> {
            if(peerGroup.getPeer().equals(peer)) {
                peerGroupsToRemove.add(peerGroup);
            }
        });
        for(PeerGroups peerGroups: peerGroupsToRemove) {
            avail.remove(peerGroups);
        }

        // TODO also remove from load balancer hash
        LOG.info("Peer {} disconnected", peer);
        LOG.info("Peers list: [{}]", peers);
        LOG.info("Balanced peers: [{}]", avail);
    };

    private final Consumer<RaftPeer> handlePeerInterrupted = peer ->
        LOG.info("Peer {} interrupted", peer);

    private final Consumer<RaftPeer> handlePeerConnected = peer -> {
        peers.add(peer);
        avail.add(new PeerGroups(peer));
        // TODO also add to load balancer hash
        LOG.info("Peer {} connected", peer);
        LOG.info("Peers list: [{}]", peers);
        LOG.info("Balanced peers: [{}]", avail);
    };

    private final Consumer<PartitionName> handlePartitionRegistering = partitionName -> {
        partitionNames.add(partitionName);
        LOG.info("Partition {} registering", partitionName);
        LOG.info("Partitions list: [{}]", partitionNames);
    };

    private final Consumer<PartitionName> handlePartitionRegistered = partitionName -> {
        partitionNames.add(partitionName);
        LOG.info("Partition {} registered", partitionName);
        LOG.info("Partitions list: [{}]", partitionNames);

//        broadcastRaftGroupInfo(
//                partitionInfoProto.getRaftGroup().getId().toString(),
//                META_GROUP_UUID,
//                META_GROUP_NAME,
//                stateMachineClass.getCanonicalName(),
//                raftConfig.getManagementPeersList().stream().map(raftPeer -> raftPeer.getId().toString()).collect(Collectors.toList()));
    };

    private ClusterState(RaftConfig raftConfig, ClusterManager clusterManager) {
        this.raftConfig = raftConfig;
        this.clusterManager = clusterManager;
        this.peerStates = new PeerStateProvider(raftConfig, handlePeerDisconnected, handlePeerInterrupted, handlePeerConnected);
        this.partitions = new PartitionProvider(raftConfig, handlePartitionRegistering, handlePartitionRegistered, partitionName -> {});
        this.phase = Phase.UNINITIALIZED;
    }

    public static ClusterState createUninitializedState(RaftConfig raftConfig, ClusterManager clusterManager) {
        return new ClusterState(raftConfig, clusterManager);
    }

    @Override
    public void initState(Object... args) throws IOException {
        // TODO load partitions
        // TODO load peerStates, but initialize all as disconnected (we need the first heartbeat to arrive first)
    }

    @Override
    public void loadFrom(ObjectInputStream in) throws IOException, ClassNotFoundException {

    }

    @Override
    public Object createSnapshot() {
        return null;
    }

    @Override
    public void clear() {

    }

//    public void addPeer(RaftPeer peer) {
//        peers.add(peer);
//        // TODO also add to load balancer hash
//    }

    public void updateHeartbeat(RaftPeer peer, long heartbeat) {
        if (peerStates.contains(peer) &&
                peerStates.get(peer).getConnectionState().equals(PeerState.ConnectionState.REMOVED)) {
            return; // ignore peer if explicitly removed
        }

        peerStates.update(PeerState.of(
                peer,
                heartbeat,
                PeerState.ConnectionState.CONNECTED
        ));
    }

    public ClusterHealth getClusterHealth() {
        List<NodeHealth> nodeHealths = peerStates.stream().map(NodeHealth::of).collect(Collectors.toList());
        val healthyNodes = nodeHealths.stream().filter(nodeHealth -> nodeHealth.getConnectionState() == NodeHealth.ConnectionState.CONNECTED).count();
        val isHealthy = healthyNodes > peerStates.size() / 2;
        return ClusterHealth.of(isHealthy, nodeHealths);
    }

    private void putStateMachineGroupMapping(RaftGroupId raftGroupId, String stateMachineClassname) {
        stateMachineGroupMapping.put(raftGroupId, stateMachineClassname);
        clusterManager.setStateMachineGroupMappingCache(stateMachineGroupMapping);
    }

    private void removeStateMachineGroupMapping(RaftGroupId raftGroupId) {
        stateMachineGroupMapping.remove(raftGroupId);
        clusterManager.setStateMachineGroupMappingCache(stateMachineGroupMapping);
    }

    public PartitionInfo addPartition(RaftGroup raftGroup, String partitionName, String stateMachineClassName) {
        PartitionName fullPartitionName = PartitionName.of(stateMachineClassName, partitionName);
        putStateMachineGroupMapping(raftGroup.getGroupId(), stateMachineClassName);
        partitions.update(PartitionInfo.of(fullPartitionName, raftGroup, stateMachineClassName, PartitionInfo.PartitionState.REGISTERING));
        return partitions.get(fullPartitionName);
    }

    public PartitionInfo detachPartition(String stateMachineClassName, String partitionName) {
        var fullPartitionName = PartitionName.of(stateMachineClassName, partitionName);
        var partition = partitions.get(fullPartitionName);
        removeStateMachineGroupMapping(partition.getRaftGroup().getGroupId());
        return partitions.detach(fullPartitionName);
    }

    public PartitionInfo setPartitionRunning(String stateMachineClassName, String partitionName) {
        PartitionName fullPartitionName = PartitionName.of(stateMachineClassName, partitionName);
        var partition = partitions.get(fullPartitionName);
        partition.setPartitionState(PartitionInfo.PartitionState.REGISTERED);
        partitions.update(partition);
        return partition;
    }

    public List<PartitionInfo> getPartitions(String stateMachineName) {
        return partitions.stream().filter(partitionInfo -> partitionInfo.getStateMachineClassname().equals(stateMachineName)).collect(Collectors.toList());
    }

    public PartitionInfo getPartition(String stateMachineName, String partitionName) {
        return partitions.get(PartitionName.of(stateMachineName, partitionName));
    }

    public List<PartitionInfo> getPartitions() {
        return partitions.stream().collect(Collectors.toList());
    }

    public Set<RaftPeer> getPeers() {
        return peers;
    }

    public Set<RaftPeer> getConnectedPeers() {
        return peerStates.stream().filter(peerState -> peerState.getConnectionState().equals(PeerState.ConnectionState.CONNECTED)).map(PeerState::getRaftPeer).collect(Collectors.toSet());
    }

    public String getRaftGroupStateMachineClassname(RaftGroupId raftGroupId) {
        return stateMachineGroupMapping.get(raftGroupId);
    }

}
