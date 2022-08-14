package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import lombok.*;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.util.concurrent.ExecutionException;

@RequiredArgsConstructor(staticName = "of") @Builder
public class NodeInfo {

    private final static String UNKNOWN_HTTP_PORT = "?";

    @Getter @NonNull private final String id;
    @Getter @NonNull private final String host;
    @Getter private final String httpPort;
    @Getter @NonNull private final String metadataPort;
    @Getter private final String storagePath;
    @Getter private final String localHostAddress;
    @Getter private final String localHostName;
    @Getter private final String remoteHostAddress;
    @Getter private final String osName;
    @Getter private final String osVersion;
    @Getter private final String springVersion;
    @Getter private final String jdkVersion;
    @Getter private final String javaVersion;
    @Getter private final String totalDiskSpace;
    @Getter private final String usableDiskSpace;
    @Getter private final String heartbeat;

    public static NodeInfo of(RaftPeer raftPeer) {
        val raftPeerHostAndPort = raftPeer.getAddress().split(":");

        return new NodeInfoBuilder()
                .id(raftPeer.getId().toString())
                .host(raftPeerHostAndPort[0])
                .httpPort(UNKNOWN_HTTP_PORT)
                .metadataPort(raftPeerHostAndPort[1])
                .build();
    }

    public static NodeInfo of(ReplicatedMetadataMap replicatedMetaDataMap) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val nodeId = replicatedMetaDataMap.get("nodeId");
        val localHostAddress = replicatedMetaDataMap.get("localHostAddress");
        val httpPort = replicatedMetaDataMap.get("httpPort");
        val metadataPort = replicatedMetaDataMap.get("metadataPort");
        val storagePath = replicatedMetaDataMap.get("storagePath");
        val localHostName = replicatedMetaDataMap.get("localHostName");
        val remoteHostAddress = replicatedMetaDataMap.get("remoteHostAddress");
        val osName = replicatedMetaDataMap.get("osName");
        val osVersion = replicatedMetaDataMap.get("osVersion");
        val springVersion = replicatedMetaDataMap.get("springVersion");
        val jdkVersion = replicatedMetaDataMap.get("jdkVersion");
        val javaVersion = replicatedMetaDataMap.get("javaVersion");
        val totalDiskSpace = replicatedMetaDataMap.get("totalDiskSpace");
        val usableDiskSpace = replicatedMetaDataMap.get("usableDiskSpace");
        val heartbeat = replicatedMetaDataMap.get("heartbeat");

        // TODO more params
        return new NodeInfoBuilder()
                .id(nodeId)
                .host(localHostAddress)
                .httpPort(httpPort)
                .metadataPort(metadataPort)
                .storagePath(storagePath)
                .localHostAddress(localHostAddress)
                .localHostName(localHostName)
                .remoteHostAddress(remoteHostAddress)
                .osName(osName)
                .osVersion(osVersion)
                .springVersion(springVersion)
                .jdkVersion(jdkVersion)
                .javaVersion(javaVersion)
                .totalDiskSpace(totalDiskSpace)
                .usableDiskSpace(usableDiskSpace)
                .heartbeat(heartbeat)
                .build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || obj.getClass() != getClass()) return false;

        return id.equals(((NodeInfo) obj).getId());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
