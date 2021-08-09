package de.umr.raft.raftlogreplicationdemo.persistence.replication.sysinfo;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.IReplicationClient;
import lombok.val;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class RaftSystemInfoClient {

    private String raftGroupName = "default-raft-grp";
    private final RaftClient raftClient;
    private final RaftConfig raftConfig;

    protected UUID getRaftGroupUUID() {
        return UUID.nameUUIDFromBytes("sys-info".getBytes(StandardCharsets.UTF_8));
    }

    @Autowired
    public RaftSystemInfoClient(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
        this.raftClient = buildRaftClient();
    }

    private RaftClient buildRaftClient() {
        RaftGroup raftGroup = raftConfig.getRaftGroup(getRaftGroupUUID());

        RaftProperties raftProperties = new RaftProperties();
        RaftClient.Builder builder = RaftClient.newBuilder()
                .setProperties(raftProperties)
                .setRaftGroup(raftGroup)
                .setClientRpc(
                    new GrpcFactory(new Parameters())
                        .newRaftClientRpc(ClientId.randomId(), raftProperties));
        return builder.build();
    }

    // attention: This is just the leader for this system info client (thus, client itself)
//    public String getLeaderId() {
//        return raftClient.getLeaderId().toString();
//    }

    // TODO one method to retrieve group info of leader and one for the current?
    public GroupListReply listRaftGroups() throws IOException {
        val groupManagementApi = raftClient.getGroupManagementApi(raftClient.getLeaderId());
        return groupManagementApi.list();
    }

    public GroupInfoReply getRaftGroupInfo(RaftGroupId raftGroupId) throws IOException {
        val groupManagementApi = raftClient.getGroupManagementApi(raftClient.getLeaderId());
        return groupManagementApi.info(raftGroupId);
    }
}
