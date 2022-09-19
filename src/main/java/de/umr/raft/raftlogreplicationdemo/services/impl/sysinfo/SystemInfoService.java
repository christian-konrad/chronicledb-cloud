package de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.*;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.replication.sysinfo.RaftSystemInfoClient;
import lombok.NonNull;
import lombok.val;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class SystemInfoService {

    private final RaftSystemInfoClient raftSystemInfoClient;
    private final RaftConfig raftConfig;
    private final ClusterManager clusterManager;
    private final ClusterMetadataReplicationClient clusterMetadataReplicationClient;

    @Autowired
    public SystemInfoService(RaftConfig raftConfig, ClusterManager clusterManager, RaftSystemInfoClient raftSystemInfoClient, ClusterMetadataReplicationClient clusterMetadataReplicationClient) {
        this.raftConfig = raftConfig;
        this.raftSystemInfoClient = raftSystemInfoClient;
        this.clusterManager = clusterManager;
        this.clusterMetadataReplicationClient = clusterMetadataReplicationClient;
    }

    public List<RaftGroupInfo> getRaftGroups() throws IOException, ExecutionException, InterruptedException {
        val raftGroupIds = raftSystemInfoClient.listRaftGroups();
        return raftGroupIds.stream().map(raftGroupId -> {
            RaftGroupInfo raftGroupInfo = null;
            try {
                raftGroupInfo = raftSystemInfoClient.getRaftGroupInfo(raftGroupId);
            } catch (NoSuchElementException | IOException | ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
            return raftGroupInfo;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        // return raftGroups.getGroupIds().stream().map(raftGroupId -> RaftGroupInfo.of(raftGroupId.toString(), raftGroupId.getUuid().toString())).collect(Collectors.toList());
    }

    public NodeInfo getNodeInfo(String nodeId) throws ExecutionException, InterruptedException, IOException {
        val replicatedMetaDataMap = ReplicatedMetadataMap.of(nodeId, this.clusterMetadataReplicationClient);
        val nodeHealth = getClusterHealth().getNodeHealths().stream().filter(n -> n.getId().equals(nodeId)).findFirst().orElse(null);
        return NodeInfo.of(replicatedMetaDataMap, nodeHealth);
    }

    public DivisionInfo getDivisionInfo(String nodeId, String groupId) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val divisionMetaDataMap = ReplicatedMetadataMap.ofDivision(DivisionInfo.createDivisionMemberId(nodeId, groupId), this.clusterMetadataReplicationClient);
        return DivisionInfo.of(nodeId, groupId, divisionMetaDataMap);
    }

    // TODO pass UUID for better access performance (avoiding raft query)
    public RaftGroupInfo getRaftGroupInfo(String raftGroupIdString) throws IOException, ExecutionException, InterruptedException {
        RaftGroupId raftGroupId = raftSystemInfoClient.getRaftGroupIdFromString(raftGroupIdString);
        //RaftGroupId raftGroupId2 = RaftGroupId.valueOf(UUID.fromString(raftGroupIdString));
        return raftSystemInfoClient.getRaftGroupInfo(raftGroupId);
    }

    public Map<String, DivisionInfo> getRaftGroupDivisions(String raftGroupId) throws ExecutionException, InterruptedException, IOException {
        // TODO poll live instead from meta map

        val groupInfo = getRaftGroupInfo(raftGroupId);
/*        val serverName = groupInfo.getServerName();

        val division = clusterManager.getDivision(serverName, RaftGroupId.valueOf(UUID.fromString(groupInfo.getUuid())));

        val memberId = division.getMemberId().toString();
        val currentTerm = division.getInfo().getCurrentTerm();
        val lastAppliedIndex = division.getInfo().getLastAppliedIndex();
        val role = division.getInfo().getCurrentRole();
        val isAlive = division.getInfo().isAlive();*/


        return groupInfo.getNodes().stream().map(node -> {
            val nodeId = node.getId();
            try {
                return getDivisionInfo(nodeId, raftGroupId);
            } catch (ExecutionException | InterruptedException | InvalidProtocolBufferException e) {
                e.printStackTrace();
                return null;
            }

        }).filter(Objects::nonNull).collect(Collectors.toMap(DivisionInfo::getNodeId, it -> it));
    }

    public NodeHealth getNodeHealth(String nodeId) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val replicatedMetaDataMap = ReplicatedMetadataMap.of(nodeId, this.clusterMetadataReplicationClient);
        return NodeHealth.of(replicatedMetaDataMap);
    }

    public List<NodeInfo> getNodes(List<RaftGroupInfo> raftGroupInfos) throws IOException {
        return raftGroupInfos.stream()
                .flatMap(raftGroupInfo -> raftGroupInfo.getNodes().stream())
                .distinct().collect(Collectors.toList());
    }

    public SystemInfo getSystemInfo() throws IOException, ExecutionException, InterruptedException {
        val raftGroups = getRaftGroups();
        //val nodes = getNodes(raftGroups);
        val nodes = raftConfig.getManagementPeersList().stream().map(NodeInfo::of).collect(Collectors.toList());
        //val nodes = clusterManager.getClusterHealth().getNodeHealths().stream().map(nodeHealth -> NodeInfo.of(nodeHealth)).collect(Collectors.toList());
        val currentNodeId = getCurrentNodeId();
//        val currentNode = nodes.stream()
//                .filter(node -> node.getId().equals(currentNodeId))
//                .findFirst().orElse(null);

        val storagePath = getStoragePath();
        return SystemInfo.of(currentNodeId, storagePath, nodes, raftGroups);
    }

    public Map<String, Map<String, String>> getAllMetaData() throws IOException, ExecutionException, InterruptedException {
        return ReplicatedMetadataMap.getFullMetaDataMap(this.clusterMetadataReplicationClient);
    }

    public ClusterHealth getClusterHealth() throws IOException, ExecutionException, InterruptedException {
//        val raftGroups = getRaftGroups();
//        val nodes = getNodes(raftGroups);
//        List<NodeHealth> nodeHealths = nodes.stream().map(nodeInfo -> {
//            try {
//                return getNodeHealth(nodeInfo.getId());
//            } catch (ExecutionException | InterruptedException | InvalidProtocolBufferException e) {
//                e.printStackTrace();
//                return null;
//            }
//        }).collect(Collectors.toList());
//        val healthyNodes = nodeHealths.stream().filter(nodeHealth -> nodeHealth.getConnectionState() == NodeHealth.ConnectionState.CONNECTED).count();
//        val isHealthy = healthyNodes > nodes.size() / 2;


        return clusterManager.getClusterHealth();
        //return ClusterHealth.of(isHealthy, nodeHealths);
    }

    private String getCurrentNodeId() {
        return raftConfig.getCurrentPeerId();
    }

    private String getStoragePath() {
        return raftConfig.getStoragePath();
    }
}
