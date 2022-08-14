package de.umr.raft.raftlogreplicationdemo.replication.api;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.PartitionInfoProto;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a partition spawned by a StateMachineProvider,
 * thus a group of nodes and the identifier of the state machine instance
 */
@RequiredArgsConstructor(staticName = "of")
public class PartitionInfo {
    @Getter @NonNull final PartitionName partitionName;
    @Getter @NonNull final RaftGroup raftGroup;
    @Getter @NonNull final String stateMachineClassname;
    @Getter @Setter PartitionState partitionState = PartitionState.UNKNOWN;

    enum PartitionState {
        UNKNOWN, REGISTERING, REGISTERED, DETACHED
    }

    public static PartitionInfo of(PartitionInfoProto partitionInfoProto, String stateMachineClassname) {
        var raftGroupProto = partitionInfoProto.getRaftGroup();

        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupProto.getId());
        List<RaftPeer> raftPeers = raftGroupProto.getPeersList().stream()
                .map(raftPeerProto -> RaftPeer.newBuilder()
                        .setId(raftPeerProto.getId())
                        .setAddress(raftPeerProto.getAddress())
                        .build())
                .collect(Collectors.toList());

        var partitionInfo = PartitionInfo.of(
                PartitionName.of(partitionInfoProto.getPartitionName()),
                RaftGroup.valueOf(raftGroupId, raftPeers),
                stateMachineClassname);

        partitionInfo.setPartitionState(PartitionState.valueOf(partitionInfoProto.getPartitionState().name()));

        return partitionInfo;
    }
}
