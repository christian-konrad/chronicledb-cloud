package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.IReplicationClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

abstract class RaftReplicationClient implements IReplicationClient.Raft {

    protected RaftClient raftClient;
    protected final RaftConfig raftConfig;

    protected abstract UUID getRaftGroupUUID();

    @Autowired
    public RaftReplicationClient(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
    }

    // must be overridden
    abstract protected RaftGroup getRaftGroup(UUID raftGroupUUID);

    private RaftClient buildRaftClient() {
        // TODO this only holds for metadata, but not for app logic server
        // TODO make it protected method
        // RaftGroup raftGroup = raftConfig.getMetadataRaftGroup(getRaftGroupUUID());
        RaftGroup raftGroup = getRaftGroup(getRaftGroupUUID());

        RaftProperties raftProperties = new RaftProperties();
        RaftClient.Builder builder = RaftClient.newBuilder()
                .setProperties(raftProperties)
                .setRaftGroup(raftGroup)
                // TODO in prod, should retry ever?
                // .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(10, TimeDuration.ONE_SECOND))
                // no retry causes client to never update the leader info, thus may fail forever
                // .setRetryPolicy(RetryPolicies.noRetry())
                .setClientRpc(
                    new GrpcFactory(new Parameters())
                        .newRaftClientRpc(ClientId.randomId(), raftProperties));
        return builder.build();
    }

    // TODO use metadata to add other peers
    // raftClient.admin().setConfiguration(peers)

    @Override
    public CompletableFuture<RaftClientReply> send(Message message) {
        if (raftClient == null) raftClient = buildRaftClient();
        return raftClient.async().send(message);
    }

    @Override
    public CompletableFuture<RaftClientReply> sendReadOnly(Message message) {
        if (raftClient == null) raftClient = buildRaftClient();
        return raftClient.async().sendReadOnly(message);
    }

    public GroupManagementApi getGroupManagementApi() {
        if (raftClient == null) raftClient = buildRaftClient();
        return raftClient.getGroupManagementApi(raftClient.getLeaderId());
    }
}
