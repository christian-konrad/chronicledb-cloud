package de.umr.raft.raftlogreplicationdemo.replication.api;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.ratis.protocol.RaftPeer;

/**
 * The state of a cluster node/raft peer
 */
@RequiredArgsConstructor(staticName = "of")
public class PeerState {
    @Getter @NonNull final RaftPeer raftPeer;
    @Getter @NonNull long lastHeartbeat;
    @Getter @Setter @NonNull ConnectionState connectionState;

    public enum ConnectionState {
        CONNECTED, // connected to the cluster (i.e. lastHeartbeat <= interruptedThreshold)
        INTERRUPTED, // currently not responding (i.e. lastHeartbeat <= disconnectedThreshold)
        DISCONNECTED, // not responding, therefore disconnected from cluster (i.e. lastHeartbeat > disconnectedThreshold)
        REMOVED // actively removed from cluster (archieved node)
    }

    @Override
    public String toString() {
        return raftPeer + "|" + connectionState + "|heartbeat@" + lastHeartbeat;
    }
}
