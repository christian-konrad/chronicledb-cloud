package de.umr.raft.raftlogreplicationdemo.models.clustermanagement;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RaftPeerResponse {
    @Getter @NonNull final String id;
    @Getter @NonNull final String address;
}
