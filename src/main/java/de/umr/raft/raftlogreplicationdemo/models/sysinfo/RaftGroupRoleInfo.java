package de.umr.raft.raftlogreplicationdemo.models.sysinfo;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class RaftGroupRoleInfo {
    // keys: nodeIds
    //@Getter @NonNull private final Map<String, RaftGroupNodeRoleInfo> nodeRoleInfos;

    public static RaftGroupRoleInfo of(RaftProtos.RoleInfoProto roleInfoProto, List<NodeInfo> nodes) {
        //Map<String, RaftGroupNodeRoleInfo> nodeRoleInfos = new HashMap<>();



        // val roleInfo = raftGroupInfoReply.getRoleInfoProto();

        // val currentLeaderTerm = roleInfo.getLeaderInfo().getTerm();

        // val selfRole = roleInfo.getRole().name();
        // val roleSince = roleInfo.getRoleElapsedTimeMs();

        // val leaderId = roleInfo.getFollowerInfo().getLeaderInfo().getId().getId().toString(Charset.defaultCharset());

        // get the first node of the (confusing) role info object
        // this object is a nested structure, starting with the node
        // responding to the groupManagementAPI (= the leader of the management group)
        // It either contains a leader with its followers nested
        // or a follower with a link to the leader (then every node else is a follower)
        // or a candidate, thus all nodes are considered candidates
        // TODO what happens in terms of network partitioning?
        // The group management API does not know if it is partitioned.
        // Therefore, must show in the UI a warning if any node is disconnected
        // and the number of reacheable nodes is smaller than the registered nodes
        // TODO then show error and do not draw the whole diagram

        // TODO only info reliably obtainable: what is the current leader
        // and retrospective network situation.
        // But current situation is not reliably obtainable
        // as for example in case of partitioning, we won't know
        // if unreacheable nodes are currently leader, follower or candidates
        // and if a node is a candidate, it does not know anything about the
        // other nodes

        // TODO better: each node sends it's own info via heartbeat / metadata service
        // so we can reliably know the situation n seconds/millis before

        val firstPeer = roleInfoProto.getSelf();
        val firstPeerId = RaftPeerId.valueOf(firstPeer.getId());
        val firstPeerRole = roleInfoProto.getRole();

        roleInfoProto.getPeerInfoCase().name();

        System.out.println("======================================");
        System.out.println("======================================");
        System.out.println("firstPeer");
        System.out.println(firstPeer);
        System.out.println("firstPeerId");
        System.out.println(firstPeerId);
        System.out.println("firstPeerRole");
        System.out.println(firstPeerRole);

        val roleElapsedTimeMs = roleInfoProto.getRoleElapsedTimeMs();
        switch (roleInfoProto.getPeerInfoCase()) {
            case LEADERINFO:
                val leaderInfo = roleInfoProto.getLeaderInfo();
                val followers = leaderInfo.getFollowerInfoList();
                //RaftGroupNodeRoleInfo.of(firstPeerId, role)
                break;
            case FOLLOWERINFO:
                val followerInfo = roleInfoProto.getFollowerInfo();
                // TODO just need it once
                if (followerInfo.hasLeaderInfo()) {
                    followerInfo.getLeaderInfo();
                };
                break;
            case CANDIDATEINFO:
                // if candidate, all are candidates
                val candidateInfo = roleInfoProto.getCandidateInfo();
                candidateInfo.getLastLeaderElapsedTimeMs();
                break;
            case PEERINFO_NOT_SET:
            default:
        }

        /*
        {
            id,
            role
        }
        */


        // TODO collect all info available per node
//      val nodeRoleInfos = nodes.stream()
//                .map(nodeInfo -> RaftGroupNodeRoleInfo.of(nodeInfo.getId()))
//                .collect(Collectors.toMap(RaftGroupNodeRoleInfo::getId, it -> it));

        return RaftGroupRoleInfo.of();
    }
}
