package de.umr.raft.raftlogreplicationdemo.config;

import lombok.Getter;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Configuration
public class RaftConfig {

    @Value("${peers}")
    @Getter private String peers;

    @Value("${node-id}")
    @Getter private String currentPeerId;

    @Value("${storage}")
    @Getter private String storagePath;

    // TODO does this belong here?
    public RaftGroup getRaftGroup(UUID raftGroupUUID) {
        List<String> peerDefinitions = Arrays.asList(getPeers().split(","));

        List<String[]> peerIdsAndAddresses = peerDefinitions.stream()
                .map(peerAddress -> peerAddress.split(":", 2))
                .collect(Collectors.toList());

        List<RaftPeer> peers = peerIdsAndAddresses.stream()
                .map(peerIdAndAddress ->
                        RaftPeer.newBuilder().setId(peerIdAndAddress[0]).setAddress(peerIdAndAddress[1]).build())
                .collect(Collectors.toList());

//        peers.add(RaftPeer.newBuilder().setId("n2").setAddress("localhost:6001").build());
//        peers.add(RaftPeer.newBuilder().setId("n3").setAddress("localhost:6002").build());

        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
        return RaftGroup.valueOf(raftGroupId, peers);
    }
}
