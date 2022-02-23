package de.umr.raft.raftlogreplicationdemo.config;

import lombok.Getter;
import lombok.val;
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

    /**
     *  If true, the events are buffered before inserting into
     *  the event store.
     *  Also see eventStoreBufferSize and eventStoreBufferTimeout
     */
    @Value("${eventstore.buffer.enabled}")
    @Getter private boolean isEventStoreBufferEnabled;

    /**
     *  Size of the event buffer of the store in bytes.
     *  Used to drastically improve write performance.
     *  Events are on hold in the buffer before beeing written to disk in a batch.
     *  Events in the buffer will be flushed if
     *  - Buffer is full
     *  - Timeout is reached
     *  - Another command than an event insert is sent
     *  ⚠ In case of any failure, all events in the buffer will be lost.
     */
    @Value("${eventstore.buffer.size}")
    @Getter private long eventStoreBufferSize;

    /**
     *  Timeout in ms of the event buffer.
     *  If the timeout is reached after the last insert attempt, the buffer is flushed
     *  even if it isn't full
     */
    @Value("${eventstore.buffer.timeout}")
    @Getter private int eventStoreBufferTimeout;

    // TODO actually, THIS is replication-port. Need also seperate, explicit port for management group
    @Value("${metadata-port}")
    @Getter private String metadataPort;
    // or name managementPort and replicationPort ?
    // TODO need some kind of port proxy

    private String[] getPeerIdAndAddress(String peerDefinition) {
        // format can be <PEER_ID>:<HOST>:<PORT> or <HOST>:<PORT> where HOST == PEER_ID
        val countOfDoublePoints = peerDefinition.chars().filter(ch -> ch == ':').count();
        if (countOfDoublePoints == 1) {
            return new String[]{peerDefinition.split(":")[0], peerDefinition};
        }
        return peerDefinition.split(":", 2);
    }

    public List<RaftPeer> getManagementPeersList() {
        List<String> peerDefinitions = Arrays.asList(getManagementPeers().split(","));

        List<String[]> peerIdsAndAddresses = peerDefinitions.stream()
                .map(this::getPeerIdAndAddress)
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
