package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.ratis.protocol.RaftPeer;

@RequiredArgsConstructor(staticName = "of")
public class NodeInfo {

    private final static String UNKNOWN_HTTP_PORT = "?";

    @Getter @NonNull private final String id;
    @Getter @NonNull private final String host;
    @Getter private final String httpPort;
    @Getter @NonNull private final String raftPort;

    public static NodeInfo of(RaftPeer raftPeer) {
        val raftPeerHostAndPort = raftPeer.getAddress().split(":");
        return new NodeInfo(raftPeer.getId().toString(), raftPeerHostAndPort[0], UNKNOWN_HTTP_PORT, raftPeerHostAndPort[1]);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || obj.getClass() != getClass()) return false;

        return id.equals(((NodeInfo) obj).getId());
    }
}
