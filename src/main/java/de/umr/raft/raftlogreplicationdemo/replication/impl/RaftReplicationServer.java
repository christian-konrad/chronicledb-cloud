package de.umr.raft.raftlogreplicationdemo.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.IReplicationServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.ReplicatedMetadataMap;
import lombok.val;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Deprecated
public abstract class RaftReplicationServer<StateMachine extends BaseStateMachine> implements IReplicationServer.Raft {

    protected static final Logger LOG =
        LoggerFactory.getLogger(RaftReplicationServer.class);

    private final RaftServer raftServer;
    protected final RaftConfig raftConfig;
    private StateMachine stateMachine;

    private final ClusterMetadataReplicationClient metaDataClient;

    private final Constructor<? extends StateMachine> stateMachineConstructor;

    protected abstract String getServerName();

    protected String getDefaultRaftGroupName() {
        return getServerName();
    }

    protected UUID getDefaultRaftGroupUUID() {
        return UUID.nameUUIDFromBytes(getDefaultRaftGroupName().getBytes(StandardCharsets.UTF_8));
    }

    public RaftReplicationServer(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient, Class<? extends StateMachine> stateMachineImpl) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        this.raftConfig = raftConfig;
        this.metaDataClient = metaDataClient;
        this.stateMachineConstructor = stateMachineImpl.getConstructor();
        this.raftServer = buildRaftServer();
    }

    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
        return RaftGroup.valueOf(raftGroupId);
    }

    private RaftServer buildRaftServer() throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        RaftGroup raftGroup = getRaftGroup(getDefaultRaftGroupUUID());

        //find current peer object based on application parameter
        RaftPeer currentPeer =
                raftGroup.getPeer(RaftPeerId.valueOf(raftConfig.getCurrentPeerId()));

        val properties = new RaftProperties();
        val raftStorageDir = new File(raftConfig.getStoragePath() + "/" + getDefaultRaftGroupUUID().toString());
        RaftServerConfigKeys.setStorageDir(properties,
                Collections.singletonList(raftStorageDir));
        val port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        //create the actual state machine
        stateMachine = stateMachineConstructor.newInstance();

        //create and start the Raft server
        return RaftServer.newBuilder()
                .setGroup(raftGroup)
                .setProperties(properties)
                .setServerId(currentPeer.getId())
                .setStateMachine(stateMachine)
//                .setStateMachineRegistry(raftGroupId -> {
//                    /*
//                    if (raftGroupId.equals(raftGroup.getGroupId())) {
//                        // return new ManagementStateMachine();
//                        return stateMachine;
//                    }
//                    */
//                    return stateMachine;
//                })
                .build();

        // TODO use raftServer.setConfiguration() for hot config changes ( new peers added etc. )

    }

    public void start() throws IOException, ExecutionException, InterruptedException {
        LOG.info("Raft server started");
        raftServer.start();
        broadcastRaftGroupInfo();
    }

    private void broadcastRaftGroupInfo() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val replicatedMetaDataMap = ReplicatedMetadataMap.of(raftConfig.getCurrentPeerId(), metaDataClient);
        var raftGroupsString = replicatedMetaDataMap.get("raftGroups");
        if (raftGroupsString == null) raftGroupsString = "";
        val newGroupId = getRaftGroup(getDefaultRaftGroupUUID()).getGroupId().toString();

        val raftGroups = raftGroupsString.split(";");
        val raftGroupsSet = new HashSet<String>(Arrays.asList(raftGroups));
        raftGroupsSet.add(newGroupId);
        raftGroupsString = String.join(";", raftGroupsSet);

        replicatedMetaDataMap.put("raftGroups", raftGroupsString);

        // add more info about raft group here
        replicatedMetaDataMap.put(newGroupId + "-UUID", getDefaultRaftGroupUUID().toString());
        replicatedMetaDataMap.put(newGroupId + "-name", getDefaultRaftGroupName());
        replicatedMetaDataMap.put(newGroupId + "-server", getServerName());
    }

}
