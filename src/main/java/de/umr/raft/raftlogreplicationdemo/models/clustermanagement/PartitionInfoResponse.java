package de.umr.raft.raftlogreplicationdemo.models.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class PartitionInfoResponse {
    @Getter @NonNull final String partitionName;
    @Getter @NonNull final String raftGroupId;
    @Getter @NonNull final List<RaftPeerResponse> peers;
    @Getter @NonNull final String stateMachineClassname;
    @Getter @NonNull final PartitionInfo.PartitionState partitionState;

    public static PartitionInfoResponse of(PartitionInfo partitionInfo) {
        return new PartitionInfoResponse(
                partitionInfo.getPartitionName().getName(),
                partitionInfo.getRaftGroup().getGroupId().toString(),
                partitionInfo.getRaftGroup().getPeers().stream().map(raftPeer -> new RaftPeerResponse(raftPeer.getId().toString(), raftPeer.getAddress())).collect(Collectors.toList()),
                partitionInfo.getStateMachineClassname(),
                partitionInfo.getPartitionState());
    }
}
