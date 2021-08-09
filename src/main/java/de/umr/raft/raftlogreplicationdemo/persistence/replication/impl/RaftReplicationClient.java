package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.IReplicationClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

abstract class RaftReplicationClient implements IReplicationClient.Raft {

    // private String raftGroupName = "default-raft-grp";
    private final RaftClient raftClient;
    private final RaftConfig raftConfig;

    protected abstract UUID getRaftGroupUUID();

    @Autowired
    public RaftReplicationClient(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
        this.raftClient = buildRaftClient();
    }

//    private RaftGroup getRaftGroup() {
//        List<String> peerDefinitions = Arrays.asList(raftConfig.getPeers().split(","));
//
//        List<String[]> peerIdsAndAddresses = peerDefinitions.stream()
//                .map(peerAddress -> peerAddress.split(":", 2))
//                .collect(Collectors.toList());
//
//        List<RaftPeer> peers = peerIdsAndAddresses.stream()
//                .map(peerIdAndAddress ->
//                        RaftPeer.newBuilder().setId(peerIdAndAddress[0]).setAddress(peerIdAndAddress[1]).build())
//                .collect(Collectors.toList());
//
////        peers.add(RaftPeer.newBuilder().setId("n2").setAddress("localhost:6001").build());
////        peers.add(RaftPeer.newBuilder().setId("n3").setAddress("localhost:6002").build());
//
//        // TODO choose named id for service, as counter-service
//        RaftGroupId raftGroupId = RaftGroupId.valueOf(getRaftGroupUUID());
//        return RaftGroup.valueOf(raftGroupId, peers);
//    }

    private RaftClient buildRaftClient() {
        RaftGroup raftGroup = raftConfig.getRaftGroup(getRaftGroupUUID());

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

    @Override
    public CompletableFuture<RaftClientReply> send(Message message) {
//        try {
            return raftClient.async().send(message);
//        } catch (NotLeaderException e) {
//            updateRaftLeader(e.getSuggestedLeader());
//        }
    }

    @Override
    public CompletableFuture<RaftClientReply> sendReadOnly(Message message) {
        return raftClient.async().sendReadOnly(message);
    }
}
