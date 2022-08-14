package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import lombok.*;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.util.concurrent.ExecutionException;

import static java.lang.System.currentTimeMillis;

@RequiredArgsConstructor(staticName = "of") @Builder
public class NodeHealth {

    // TODO just use PeerState!

    public enum ConnectionState {
        CONNECTED, INTERRUPTED, DISCONNECTED
    }

    @Getter @NonNull private final String id;
    @Getter private final String heartbeat;
    @Getter private final ConnectionState connectionState;

    public static NodeHealth of(ReplicatedMetadataMap replicatedMetaDataMap) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val nodeId = replicatedMetaDataMap.get("nodeId");
        val heartbeat = replicatedMetaDataMap.get("heartbeat");
        val heartbeatLong = heartbeat != null ? Long.parseLong(heartbeat) : 0;

        // TODO make those thresholds configurable in .properties
        val secondsSinceLastHeartbeat = (currentTimeMillis() - heartbeatLong) / 1000;
        val connectionState = secondsSinceLastHeartbeat > 30 ? ConnectionState.DISCONNECTED
                : secondsSinceLastHeartbeat > 10 ? ConnectionState.INTERRUPTED
                : ConnectionState.CONNECTED;

        return new NodeHealth.NodeHealthBuilder()
                .id(nodeId)
                .heartbeat(heartbeat)
                .connectionState(connectionState)
                .build();
    }
}
