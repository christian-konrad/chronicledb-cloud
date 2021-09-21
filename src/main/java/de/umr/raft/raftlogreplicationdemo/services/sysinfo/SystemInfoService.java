package de.umr.raft.raftlogreplicationdemo.services.sysinfo;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.NodeInfo;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.SystemInfo;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.facades.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.sysinfo.RaftSystemInfoClient;
import lombok.val;
import org.apache.ratis.protocol.GroupInfoReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class SystemInfoService {

    private final RaftSystemInfoClient raftSystemInfoClient;
    private final RaftConfig raftConfig;
    private final ClusterMetadataReplicationClient clusterMetadataReplicationClient;

    @Autowired
    public SystemInfoService(RaftConfig raftConfig, RaftSystemInfoClient raftSystemInfoClient, ClusterMetadataReplicationClient clusterMetadataReplicationClient) {
        this.raftConfig = raftConfig;
        this.raftSystemInfoClient = raftSystemInfoClient;
        this.clusterMetadataReplicationClient = clusterMetadataReplicationClient;
    }

    public List<RaftGroupInfo> getRaftGroups() throws IOException, ExecutionException, InterruptedException {
        val raftGroupIds = raftSystemInfoClient.listRaftGroups();
        return raftGroupIds.stream().map(raftGroupId -> {
            RaftGroupInfo raftGroupInfo = null;
            try {
                raftGroupInfo = raftSystemInfoClient.getRaftGroupInfo(raftGroupId);
            } catch (IOException | ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
            return raftGroupInfo;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        // return raftGroups.getGroupIds().stream().map(raftGroupId -> RaftGroupInfo.of(raftGroupId.toString(), raftGroupId.getUuid().toString())).collect(Collectors.toList());
    }

    public NodeInfo getNodeInfo(String nodeId) throws ExecutionException, InterruptedException {
        val replicatedMetaDataMap = ReplicatedMetadataMap.of(nodeId, this.clusterMetadataReplicationClient);
        return NodeInfo.of(replicatedMetaDataMap);
    }

    public List<NodeInfo> getNodes(List<RaftGroupInfo> raftGroupInfos) throws IOException {
        return raftGroupInfos.stream()
                .flatMap(raftGroupInfo -> raftGroupInfo.getNodes().stream())
                .distinct().collect(Collectors.toList());
    }

    public SystemInfo getSystemInfo() throws IOException, ExecutionException, InterruptedException {
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
