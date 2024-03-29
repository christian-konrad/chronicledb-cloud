package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementMultiRaftServer;
import lombok.Getter;
import org.apache.ratis.protocol.RaftGroup;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Deprecated
public class LegacyCounterReplicationClient extends RaftReplicationClient<CounterOperationMessage> {

    @Getter private final String counterId;

    public static CounterOperationMessage createIncrementOperationMessage() {
        return CounterOperationMessage.Factory.createIncrementOperationMessage();
    }

    public static CounterOperationMessage createGetOperationMessage() {
        return CounterOperationMessage.Factory.createGetOperationMessage();
    }

    // TODO to know which counter, this client must ask meta cluster in advance
    // TODO even for having a singleton counter, must implement orchestration by meta cluster
    // REST Service must ask meta cluster for given counter (each counter has an id)
    // meta state machine will return existing counter or create new one (including new raft grouo)
    // later, we will replace counter by log (and finally event stream)
    // TODO on creation of new counters/logs/event streams, use (interchangeable) load balancing
    // strategy/heuristic to choose nodes for this partition (use partition.nodes.max from .properties
    // // may simply use MinHeap queue like in example

    @Override
    protected UUID getRaftGroupUUID() {
        // TODO use the right server
        return UUID.nameUUIDFromBytes((String.format("%s:%s", ClusterManagementMultiRaftServer.SERVER_NAME, counterId)).getBytes(StandardCharsets.UTF_8));
    }

    // TODO should ask metaclient which nodes are participating in this group
//    @Override
//    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
//        String host = raftConfig.getHostAddress();
//
//        // TODO get peers from metadata client
//        val peer = RaftPeer.newBuilder().setId(raftConfig.getCurrentPeerId()).setAddress(host + ":" + raftConfig.getMetadataPort()).build();
//
//        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
//        return RaftGroup.valueOf(raftGroupId, peer);
//    }

    @Override
    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        return raftConfig.getManagementRaftGroup(getRaftGroupUUID());
    }

    @Autowired
    public LegacyCounterReplicationClient(RaftConfig raftConfig, String counterId) {
        super(raftConfig);
        this.counterId = counterId;
    }
}
