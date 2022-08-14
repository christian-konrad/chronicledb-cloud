package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionName;
import de.umr.raft.raftlogreplicationdemo.replication.api.PeerState;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.data.StateMachineState;
import lombok.Getter;
import org.apache.ratis.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ClusterState implements StateMachineState {

    private final RaftConfig raftConfig;

    @Getter
    private StateMachineState.Phase phase;

    // List of the currently known peers, built upon heartbeats
    private final Set<RaftPeer> peers = new HashSet<>();

    // And their health states
    private final PeerStateProvider peerStates;

    // TODO heartbeat messages for clustermanager
    // TODO print peers list as it grows on log

    // TODO healthCheck daemon (where?) to replace client-triggered-only health checks

    // all registered raft groups
    //private final Set<StateMachineProvider> stateMachineProviders = new HashSet<>();
    private final Map<PartitionName, PartitionInfo> partitions = new ConcurrentHashMap<>();

    // TODO also index map for statemachines?

    // TODO also raft groups? or only raft groups?

    // TODO everything else that should be known
    // - lastHeartbeats
    // - raft groups registered to peers
    // - raft groups registered in general

    // TODO lastStartedAt

    protected static final Logger LOG =
            LoggerFactory.getLogger(ClusterState.class);

    private final Consumer<RaftPeer> handlePeerDisconnected = peer -> {
        // TODO also on disconnected, remove peer from it's partitions via adminAPI of the corresponding groups (therefore, need clients)
        // TODO also remove from loadBalancer list
        // TODO if it comes back again, re-add it
        peers.remove(peer);
        // TODO also remove from load balancer hash
        LOG.info("Peer {} disconnected", peer);
        LOG.info("Peers list: [{}]", peers);
    };

    private final Consumer<RaftPeer> handlePeerInterrupted = peer ->
        LOG.info("Peer {} interrupted", peer);

    private final Consumer<RaftPeer> handlePeerConnected = peer -> {
        peers.add(peer);
        // TODO also add to load balancer hash
        LOG.info("Peer {} connected", peer);
        LOG.info("Peers list: [{}]", peers);
    };

    private ClusterState(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
        this.peerStates = new PeerStateProvider(raftConfig, handlePeerDisconnected, handlePeerInterrupted, handlePeerConnected);
        this.phase = Phase.UNINITIALIZED;
    }

    public static ClusterState createUninitializedState(RaftConfig raftConfig) {
        return new ClusterState(raftConfig);
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
}
