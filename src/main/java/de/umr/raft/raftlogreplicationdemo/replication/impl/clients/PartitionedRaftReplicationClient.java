package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementMultiRaftServer;
import lombok.Getter;
import org.apache.ratis.protocol.RaftGroup;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class PartitionedRaftReplicationClient<ExecutableMessageImpl extends ExecutableMessage> extends RaftReplicationClient<ExecutableMessageImpl> {

    @Getter private final String partitionId;

    // TODO to know which partition, this client must ask meta cluster in advance
    // TODO even for having a singleton partition, must implement orchestration by meta cluster
    // REST Service must ask meta cluster for given partition (each partition has an id)
    // meta state machine will return existing partition or create new one (including new raft grouo)
    // later, we will replace counter by log (and finally event stream)
    // TODO on creation of new counters/logs/event streams, use (interchangeable) load balancing
    // strategy/heuristic to choose nodes for this partition (use partition.nodes.max from .properties
    // // may simply use MinHeap queue like in example

    @Override
    protected UUID getRaftGroupUUID() {
        // TODO should use SERVER_NAME:STATE_MACHINE_NAME:PARTITION_ID
        // TODO use the right server
        return UUID.nameUUIDFromBytes((String.format("%s:%s", ClusterManagementMultiRaftServer.SERVER_NAME, partitionId)).getBytes(StandardCharsets.UTF_8));
    }

    // TODO should ask metaclient which nodes are participating in this group/partition
//    @Override
//    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
//        String host = raftConfig.getHostAddress();
//
//        // TODO get peers from metadata client
//        val peer = RaftPeer.newBuilder().setId(raftConfig.getCurrentPeerId()).setAddress(host + ":" + raftConfig.getReplicationPort()).build();
//
//        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
//        return RaftGroup.valueOf(raftGroupId, peer);
//    }

    // TODO uses all nodes, just for testing/developing; should use only replica.count nodes for partition
    @Override
    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        return raftConfig.getManagementRaftGroup(getRaftGroupUUID());
    }

    @Autowired
    public PartitionedRaftReplicationClient(RaftConfig raftConfig, String partitionId) {
        super(raftConfig);
        this.partitionId = partitionId;
    }
}
