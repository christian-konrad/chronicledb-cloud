package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.IReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedEventStore;
import lombok.val;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public abstract class RaftReplicationClient<ExecutableMessageImpl extends ExecutableMessage> implements IReplicationClient.Raft {

    protected RaftClient raftClient;
    protected final RaftConfig raftConfig;

    protected abstract UUID getRaftGroupUUID();

    Logger LOG = LoggerFactory.getLogger(RaftReplicationClient.class);

    @Autowired
    public RaftReplicationClient(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
    }

    // must be overridden
    abstract protected RaftGroup getRaftGroup(UUID raftGroupUUID);

    // TODO return future and use executeThreadPool?
    public <MessageResultProto extends org.apache.ratis.thirdparty.com.google.protobuf.Message> MessageResultProto sendAndExecuteOperationMessage(ExecutableMessageImpl message, Parser<MessageResultProto> resultProtoParser) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        Message response;

        //Instant start = Instant.now();
        if (message.isTransactionMessage()) {
            response = send(message).get().getMessage();
        } else {
            response = sendReadOnly(message).get().getMessage();
        }
        //Instant finish = Instant.now();
        //long timeElapsed = Duration.between(start, finish).toMillis();
        //LOG.info("Sending and receiving message took " + timeElapsed + "ms");

          // makes no difference
//        try {
//            if (message.isTransactionMessage()) {
//                response = sendBlocking(message).getMessage();
//            } else {
//                response = sendReadOnlyBlocking(message).getMessage();
//            }
//        } catch (Exception e) { // TODO bad practice
//            throw new RuntimeException(e);
//        }

        return resultProtoParser.parseFrom(response.getContent());
    }

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

    public RaftClientReply sendBlocking(Message message) throws IOException {
        if (raftClient == null) raftClient = buildRaftClient();
        return raftClient.io().send(message);
    }

    public RaftClientReply sendReadOnlyBlocking(Message message) throws IOException {
        if (raftClient == null) raftClient = buildRaftClient();
        return raftClient.io().sendReadOnly(message);
    }

    public GroupManagementApi getGroupManagementApi() {
        if (raftClient == null) raftClient = buildRaftClient();
        return raftClient.getGroupManagementApi(raftClient.getLeaderId());
    }
}
