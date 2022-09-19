package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.serialization;

import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;

import java.util.stream.Collectors;

public class PartitionInfoProtoSerializer {

    public static PartitionInfoProto toProto(PartitionInfo partitionInfo) {
        return PartitionInfoProto.newBuilder()
                .setPartitionName(PartitionNameProto.newBuilder()
                        .setName(partitionInfo.getPartitionName().getName())
                        .setStateMachineClassName(partitionInfo.getPartitionName().getStateMachineClassName())
                        .build())
                .setPartitionState(PartitionState.valueOf(partitionInfo.getPartitionState().name()))
                .setRaftGroup(RaftGroupProto.newBuilder()
                        .setId(partitionInfo.getRaftGroup().getGroupId().toByteString())
                        .setUuid(partitionInfo.getRaftGroup().getGroupId().getUuid().toString())
                        .setName(partitionInfo.getPartitionName().getName())
                        .addAllPeers(partitionInfo.getRaftGroup().getPeers().stream().map(raftPeer -> RaftPeerProto.newBuilder()
                                .setId(raftPeer.getRaftPeerProto().getId())
                                .setAddress(raftPeer.getAddress())
                                .build()).collect(Collectors.toList()))
                        .build())
                .build();
    }

}
