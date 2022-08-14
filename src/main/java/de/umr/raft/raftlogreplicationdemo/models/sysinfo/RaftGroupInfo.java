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
    // @Getter @NonNull private final String leaderId;
    // @Getter @NonNull private final long currentLeaderTerm;
    // @Getter @NonNull private final String selfRole;
    // @Getter @NonNull private final long roleSince;
    @Getter @NonNull private final boolean isStorageHealthy;
    @Getter @NonNull private final String stateMachineClass;
    @Getter @NonNull private final String serverName;
    //@Getter @NonNull private final String roleInfo;
    //@Getter @NonNull private final RaftGroupRoleInfo raftGroupRoleInfo;

    // TODO state machine instance info of this group

    // TODO draw as diagram with states of nodes
    //  therefore, need state per node in this group
    //  TODO also return peer priority and admin, client and datastream address

    public static RaftGroupInfo of(GroupInfoReply raftGroupInfoReply, String raftGroupName, String stateMachineClass, String serverName) {
        val raftGroup = raftGroupInfoReply.getGroup();
        val raftGroupId = raftGroup.getGroupId();
        // TODO division?



        val peers = raftGroup.getPeers();
        val peersAsNodeInfo = peers.stream().map(NodeInfo::of).collect(Collectors.toList());
        // TODO division info at this point
        val isStorageHealthy = raftGroupInfoReply.isRaftStorageHealthy();

        val roleInfo = raftGroupInfoReply.getRoleInfoProto();

        //val raftGroupRoleInfo = RaftGroupRoleInfo.of(roleInfo, peersAsNodeInfo);

        return new RaftGroupInfo(raftGroupName, raftGroupId.toString(), raftGroupId.getUuid().toString(), peersAsNodeInfo, isStorageHealthy, stateMachineClass, serverName);
    }

    public static RaftGroupInfo of(GroupInfoReply raftGroupInfoReply) {
        return RaftGroupInfo.of(raftGroupInfoReply, raftGroupInfoReply.getGroup().getGroupId().toString(), "", "");
    }
}
