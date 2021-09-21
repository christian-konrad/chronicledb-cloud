package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor(staticName = "of")
public class SystemInfo {

    @Getter private final String currentNodeId;
    @Getter private final String currentNodeStoragePath;
    @Getter private final List<NodeInfo> nodes;
    @Getter private final List<RaftGroupInfo> raftGroups;

    // TODO
    /*
    raftServer.getDivision().getRaftServerMetrics()
    raftServer.getDivision().getInfo()
    raftServer.getDivision().getRaftConf()
    raftServer.getDivision().getStateMachine()
    raftServer.getDivision().getRaftLog()
     */
}
