package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.PeerState;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PeerStateProvider implements Iterable<PeerState> {

    private static final Logger LOG = LoggerFactory.getLogger(PeerStateProvider.class);

    // daemon handling heartbeats and connection states
    private final Daemon peerHealthChecker;

    private final RaftConfig raftConfig;
    private final Consumer<RaftPeer> onPeerDisconnected;
    private final Consumer<RaftPeer> onPeerInterrupted;
    private final Consumer<RaftPeer> onPeerConnected;

    public PeerStateProvider(RaftConfig raftConfig, Consumer<RaftPeer> onPeerDisconnected, Consumer<RaftPeer> onPeerInterrupted, Consumer<RaftPeer> onPeerConnected) {
        this.raftConfig = raftConfig;
        this.onPeerDisconnected = onPeerDisconnected;
        this.onPeerInterrupted = onPeerInterrupted;
        this.onPeerConnected = onPeerConnected;

        peerHealthChecker = new Daemon(new PeerHealthChecker(), PeerHealthChecker.DAEMON_NAME);
        peerHealthChecker.start();
    }

    private final Map<RaftPeer, PeerState> peerStates = new HashMap<>();
//
//    // At the same time, manage minHeap queue for load balancing partitions across the peers
//    private PriorityBlockingQueue<PartitionsPerPeer> balancedPeers = new PriorityBlockingQueue<PartitionsPerPeer>();
//
//    static class PartitionsPerPeer implements Comparable {
//        private RaftPeer peer;
//        private Set<PartitionInfo> partitions = new HashSet<>();
//
//        PartitionsPerPeer(RaftPeer peer) {
//            this.peer = peer;
//        }
//
//        public Set<PartitionInfo> getPartitions() {
//            return partitions;
//        }
//
//        public RaftPeer getPeer() {
//            return peer;
//        }
//
//        @Override
//        public int compareTo(Object o) {
//            return partitions.size() - ((PartitionsPerPeer) o).partitions.size();
//        }
//    }

    // TODO peer ports on app logic server?

    // TODO also allow adding partitions here
    public void update(PeerState peerState) {
        var peer = peerState.getRaftPeer();
        var previousState = peerStates.get(peer);

        var newConnectionState = peerState.getConnectionState();

        peerStates.put(peer, peerState);

        if (previousState == null || !previousState.getConnectionState().equals(newConnectionState)) {
            handleConnectionStateChanged(peer, newConnectionState);
        }

        // LOG.info("Peer states: {}", peerStates);
    }

    private void handleConnectionStateChanged(RaftPeer peer, PeerState.ConnectionState connectionState) {
        switch (connectionState) {
            case DISCONNECTED:
                onPeerDisconnected.accept(peer);
                break;
            case INTERRUPTED:
                onPeerInterrupted.accept(peer);
                break;
            case CONNECTED:
                onPeerConnected.accept(peer);
        }
    }

    public void updateConnectionState(RaftPeer peer, PeerState.ConnectionState connectionState) {
        var peerState = peerStates.get(peer);

        // connection state unchanged
        if (peerState.getConnectionState().equals(connectionState)) return;

        // connection state changed
        handleConnectionStateChanged(peer, connectionState);
        peerState.setConnectionState(connectionState);
    }

    public PeerState get(RaftPeer peer) {
        return Optional.ofNullable(peerStates.get(peer))
                .orElseThrow(() -> new IllegalArgumentException("No state for peer " + peer.toString()) );
    }

    public boolean contains(RaftPeer peer) {
        return peerStates.containsKey(peer);
    }

    PeerState remove(RaftPeer peer) {
        update(PeerState.of(peer, -1, PeerState.ConnectionState.REMOVED));
        return peerStates.get(peer);
    }

    public Set<Map.Entry<RaftPeer, PeerState>> entrySet() {
        return peerStates.entrySet();
    }

    @Override
    public Iterator<PeerState> iterator() {
        return peerStates.values().iterator();
    }

    public Stream<PeerState> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public int size() {
        return peerStates.size();
    }

    @Override
    public String toString() {
        return "{" +
            peerStates.values().stream().map(PeerState::toString).collect(Collectors.joining(", ")) +
                "}";
    }

    // should this run on each node?
    // TODO is this the best and safest way to have a daemon like this running (for very long times)?
    private class PeerHealthChecker implements Runnable {

        static final String DAEMON_NAME = "peer-health-checker";

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(1000);
                    long now = System.currentTimeMillis();

                    peerStates.keySet().forEach(raftPeer -> {
                        PeerState peerState = peerStates.get(raftPeer);

                        long msSinceLastHeartbeat = now - peerState.getLastHeartbeat();

                        if (msSinceLastHeartbeat > raftConfig.getDisconnectedThreshold()) {
                            updateConnectionState(raftPeer, PeerState.ConnectionState.DISCONNECTED);
                        } else if (msSinceLastHeartbeat > raftConfig.getInterruptedThreshold()) {
                            updateConnectionState(raftPeer, PeerState.ConnectionState.INTERRUPTED);
                        } else {
                            updateConnectionState(raftPeer, PeerState.ConnectionState.CONNECTED);
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Exception while managing cluster health", e);
                }
            }
        }
    }
}
