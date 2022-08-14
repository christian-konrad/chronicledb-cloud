package de.umr.raft.raftlogreplicationdemo.models.clustermanagement;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import java.util.Optional;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class RaftPeerRequest {
    @Getter @NonNull final String id;
    @Getter @NonNull final String address;

    public RaftPeer getRaftPeer() {
        return RaftPeer.newBuilder()
                .setId(id)
                .setAddress(address)
                .build();
    }

}
