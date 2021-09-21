package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import lombok.*;
import org.apache.ratis.protocol.GroupInfoReply;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class RaftGroupInfo {

    @Getter @NonNull private final String name;
    @Getter @NonNull private final String groupId;
    @Getter @NonNull private final String uuid;
    @Getter @NonNull private final List<NodeInfo> nodes;
    @Getter @NonNull private final String leaderId;
    @Getter @NonNull private final long currentLeaderTerm;
    @Getter @NonNull private final String selfRole;
    @Getter @NonNull private final long roleSince;
    @Getter @NonNull private final boolean isStorageHealthy;
    @Getter @NonNull private final String stateMachineClass;
    @Getter @NonNull private final String serverName;

    // TODO state machine instance info of this group

    public static RaftGroupInfo of(GroupInfoReply raftGroupInfoReply, String raftGroupName, String stateMachineClass, String serverName) {
        val raftGroup = raftGroupInfoReply.getGroup();
        val raftGroupId = raftGroup.getGroupId();
        val peers = raftGroup.getPeers();
        val peersAsNodeInfo = peers.stream().map(NodeInfo::of).collect(Collectors.toList());
        val isStorageHealthy = raftGroupInfoReply.isRaftStorageHealthy();
        val currentLeaderTerm = raftGroupInfoReply.getRoleInfoProto().getLeaderInfo().getTerm();

        val roleInfo = raftGroupInfoReply.getRoleInfoProto();

        val selfRole = roleInfo.getRole().name();
        val roleSince = roleInfo.getRoleElapsedTimeMs();

        val leaderId = roleInfo.getFollowerInfo().getLeaderInfo().getId().getId().toString(Charset.defaultCharset());

        return new RaftGroupInfo(raftGroupName, raftGroupId.toString(), raftGroupId.getUuid().toString(), peersAsNodeInfo, leaderId, currentLeaderTerm, selfRole, roleSince, isStorageHealthy, stateMachineClass, serverName);
    }

    public static RaftGroupInfo of(GroupInfoReply raftGroupInfoReply) {
        return RaftGroupInfo.of(raftGroupInfoReply, raftGroupInfoReply.getGroup().getGroupId().toString(), "", "");
    }
}
