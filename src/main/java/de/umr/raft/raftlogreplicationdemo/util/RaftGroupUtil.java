package de.umr.raft.raftlogreplicationdemo.util;

import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import lombok.val;

import java.util.List;
import java.util.stream.Collectors;

public class RaftGroupUtil {

    public static List<RaftGroupInfo> filterRaftGroupsByStateMachine(List<RaftGroupInfo> raftGroups, Class stateMachineClass) {
        return raftGroups.stream().filter(raftGroupInfo -> {
            val thisStateMachineClassName = raftGroupInfo.getStateMachineClass();
            val eventStoreStateMachineClassName = stateMachineClass.getCanonicalName();
            return thisStateMachineClassName.equals(eventStoreStateMachineClassName);
        }).collect(Collectors.toList());
    }

    public static List<String> getPartitionNamesFromRaftGroupInfos(List<RaftGroupInfo> raftGroups) {
        return raftGroups.stream().map(raftGroupInfo ->
                raftGroupInfo.getName().replace(String.format("%s:", raftGroupInfo.getServerName()), "")
        ).collect(Collectors.toList());
    }

}
