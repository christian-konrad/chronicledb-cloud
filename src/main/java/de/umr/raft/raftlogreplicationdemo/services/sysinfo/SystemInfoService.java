package de.umr.raft.raftlogreplicationdemo.services.sysinfo;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.NodeInfo;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.SystemInfo;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.sysinfo.RaftSystemInfoClient;
import lombok.val;
import org.apache.ratis.protocol.GroupInfoReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class SystemInfoService {

    @Autowired
    RaftSystemInfoClient raftSystemInfoClient;

    @Autowired RaftConfig raftConfig;

    public List<RaftGroupInfo> getRaftGroups() throws IOException {
        val raftGroups = raftSystemInfoClient.listRaftGroups();
        return raftGroups.getGroupIds().stream().map(raftGroupId -> {
            GroupInfoReply raftGroupInfo = null;
            try {
                raftGroupInfo = raftSystemInfoClient.getRaftGroupInfo(raftGroupId);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return RaftGroupInfo.of(raftGroupInfo);
        }).collect(Collectors.toList());
        // return raftGroups.getGroupIds().stream().map(raftGroupId -> RaftGroupInfo.of(raftGroupId.toString(), raftGroupId.getUuid().toString())).collect(Collectors.toList());
    }

    public List<NodeInfo> getNodes(List<RaftGroupInfo> raftGroupInfos) throws IOException {
        return raftGroupInfos.stream()
                .flatMap(raftGroupInfo -> raftGroupInfo.getNodes().stream())
                .distinct().collect(Collectors.toList());
    }

    public SystemInfo getSystemInfo() throws IOException {
        val raftGroups = getRaftGroups();
        val nodes = getNodes(raftGroups);
        val currentNodeId = getCurrentNodeId();
//        val currentNode = nodes.stream()
//                .filter(node -> node.getId().equals(currentNodeId))
//                .findFirst().orElse(null);

        val storagePath = getStoragePath();
        return SystemInfo.of(currentNodeId, storagePath, nodes, raftGroups);
    }

    private String getCurrentNodeId() {
        return raftConfig.getCurrentPeerId();
    }

    private String getStoragePath() {
        return raftConfig.getStoragePath();
    }
}
