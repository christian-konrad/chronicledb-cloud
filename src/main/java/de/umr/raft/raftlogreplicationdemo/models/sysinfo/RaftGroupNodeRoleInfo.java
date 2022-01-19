package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

// contains map of nodes with info


@RequiredArgsConstructor(staticName = "of")
public class RaftGroupNodeRoleInfo {
    @Getter @NonNull private final String id;


}
