package de.umr.raft.raftlogreplicationdemo.config;

import lombok.Getter;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Configuration
public class RaftConfig {

    // get rid of IDs here
    @Value("${peers}")
    @Getter private String managementPeers;

    @Value("${node-id}")
    @Getter private String currentPeerId;

    @Value("${storage}")
    @Getter private String storagePath;

    @Value("${server.port}")
    @Getter private String httpPort;

    @Value("${server.address}")
    private String host;

    @Value("${heartbeat.interval}")
    @Getter private int heartbeatInterval;

    // TODO should somehow obfuscate or combine metadata and actual raft port
    @Value("${metadata-port}")
    @Getter private String metadataPort;
    // or name managementPort and replicationPort ? What if db adapter port will be added? How to handle 4 ports??
    // TODO need some kind of port proxy

    @Value("${replication-port}")
    @Getter private String replicationPort;

    public List<RaftPeer> getManagementPeersList() {
        List<String> peerDefinitions = Arrays.asList(getManagementPeers().split(","));

        List<String[]> peerIdsAndAddresses = peerDefinitions.stream()
                .map(peerAddress -> peerAddress.split(":", 2))
                .collect(Collectors.toList());

        return peerIdsAndAddresses.stream()
                .map(peerIdAndAddress ->
                        // TODO auto gen IDs using host and port
                        RaftPeer.newBuilder().setId(peerIdAndAddress[0]).setAddress(peerIdAndAddress[1]).build())
                .collect(Collectors.toList());
    }

    // TODO does this belong here?
    public RaftGroup getManagementRaftGroup(UUID raftGroupUUID) {
        List<RaftPeer> peers = getManagementPeersList();

        // peers.add(RaftPeer.newBuilder().setId("n2").setAddress("localhost:6001").build());

        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
        return RaftGroup.valueOf(raftGroupId, peers);
    }

    public String getHostAddress() {
        if (!host.isEmpty()) return host;
        return InetAddress.getLoopbackAddress().getHostAddress();
    }
}
