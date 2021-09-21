package de.umr.raft.raftlogreplicationdemo.persistence.replication.sysinfo;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.ClusterManagementMultiRaftServer;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.ClusterManagementServer;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.facades.ReplicatedMetadataMap;
import lombok.val;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class RaftSystemInfoClient {

    private final RaftConfig raftConfig;
    private final ClusterMetadataReplicationClient metaDataClient;
    private final RaftClient appLogicRaftClient;

    protected UUID getRaftGroupUUID() {
        return UUID.nameUUIDFromBytes("sys-info".getBytes(StandardCharsets.UTF_8));
    }

    @Autowired
    public RaftSystemInfoClient(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient) {
        this.raftConfig = raftConfig;
        this.metaDataClient = metaDataClient;
        this.appLogicRaftClient = buildAppLogicRaftClient();
    }

    private RaftClient buildAppLogicRaftClient() {
        RaftGroupId raftGroupId = RaftGroupId.valueOf(getRaftGroupUUID());
        String host = raftConfig.getHostAddress();
        val peer = RaftPeer.newBuilder().setId(raftConfig.getCurrentPeerId()).setAddress(host + ":" + raftConfig.getReplicationPort()).build();
        RaftGroup raftGroup =  RaftGroup.valueOf(raftGroupId, peer);

        RaftProperties raftProperties = new RaftProperties();
        RaftClient.Builder builder = RaftClient.newBuilder()
                .setProperties(raftProperties)
                .setRaftGroup(raftGroup)
                .setClientRpc(
                    new GrpcFactory(new Parameters())
                        .newRaftClientRpc(ClientId.randomId(), raftProperties));
        return builder.build();
    }

    public List<RaftGroupId> listRaftGroups() throws IOException, ExecutionException, InterruptedException {
        // TODO should metaServer encapsule logics for spawning new groups on replServer?
        // TODO so metaServer is responsible to trigger config changges on replServer after receiving a "NODE_JOINED" event by a recently started node

        // TODO iterate over groupIds from metaClient; may spawn new clients to access each group in getRaftGroupInfo
        // TODO iterate over all peers
        // TODO should have list of all available peers in replicatedMetaDataMap, too
        // as it can change over time due to config changes
        val replicatedMetaDataMap = ReplicatedMetadataMap.ofRaftGroupScope(metaDataClient);
        val raftGroupsString = replicatedMetaDataMap.get("groups");

        if (raftGroupsString == null) {
            val groupManagementApi = metaDataClient.getGroupManagementApi();
            return groupManagementApi.list().getGroupIds();
        }

        val raftGroupIds = raftGroupsString.split(";");
        val raftGroupUUIDs = new ArrayList<String>();

        for (val raftGroupId : raftGroupIds) {
            raftGroupUUIDs.add(replicatedMetaDataMap.get(raftGroupId + "-UUID"));
        }

        return raftGroupUUIDs.stream()
                .filter(Objects::nonNull)
                .map(raftGroupUUID -> RaftGroupId.valueOf(UUID.fromString(raftGroupUUID)))
                .collect(Collectors.toList());
    }

    public RaftGroupInfo getRaftGroupInfo(RaftGroupId raftGroupId) throws IOException, ExecutionException, InterruptedException {
        val replicatedMetaDataMap = ReplicatedMetadataMap.ofRaftGroupScope(metaDataClient);
        val raftServerName = replicatedMetaDataMap.get(raftGroupId.toString() + "-server");

        GroupManagementApi groupManagementApi;

        if (ClusterManagementMultiRaftServer.SERVER_NAME.equals(raftServerName)) {
            groupManagementApi = metaDataClient.getGroupManagementApi();
        } else {
            groupManagementApi = appLogicRaftClient.getGroupManagementApi(appLogicRaftClient.getLeaderId());
        }

        try {
            val raftGroupName = replicatedMetaDataMap.get(raftGroupId + "-name");
            val stateMachineType = replicatedMetaDataMap.get(raftGroupId + "-sm-class");
            val serverName = replicatedMetaDataMap.get(raftGroupId + "-server");
            return RaftGroupInfo.of(groupManagementApi.info(raftGroupId), raftGroupName, stateMachineType, serverName);
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
            return null;
        }
    }
}
