package de.umr.raft.raftlogreplicationdemo.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.replication.IReplicationServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.ClusterManagementStateMachine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.providers.StateMachineProvider;
import lombok.Getter;
import lombok.val;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class ClusterManagementServer implements IReplicationServer.Raft {

    // TODO if this runs well, then also run the app logic server again

    private static final Logger LOG =
        LoggerFactory.getLogger(ClusterManagementServer.class);

    private final RaftServer raftServer;
    private final RaftConfig raftConfig;
    private StateMachine clusterManagementStateMachine;

    @Getter private boolean isRunning = false;

    public static final String SERVER_NAME = "cluster-management";

    public static final String BASE_GROUP_NAME = SERVER_NAME + ":base";
    public static final UUID BASE_GROUP_UUID = UUID.nameUUIDFromBytes(BASE_GROUP_NAME.getBytes(StandardCharsets.UTF_8));
    public static final RaftGroupId BASE_GROUP_ID = RaftGroupId.valueOf(BASE_GROUP_UUID);

    @Autowired
    public ClusterManagementServer(RaftConfig raftConfig) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException, NoSuchMethodException {
        this.raftConfig = raftConfig;
        this.raftServer = buildRaftServer();
    }

    /**
     * Returns the base raft group responsible for handling the cluster management
     */
    private RaftGroup getBaseRaftGroup() {
        return raftConfig.getManagementRaftGroup(BASE_GROUP_UUID);
    }

    private RaftServer buildRaftServer() throws IOException {
        RaftGroup baseRaftGroup = getBaseRaftGroup();

        clusterManagementStateMachine = new ClusterManagementStateMachine(raftConfig);

        // find current peer object based on application parameter
        RaftPeer currentPeer =
                baseRaftGroup.getPeer(RaftPeerId.valueOf(raftConfig.getCurrentPeerId()));

        LOG.info("Current peer id: {}", raftConfig.getCurrentPeerId());
        LOG.info("Current peer address: {}", currentPeer.getAddress());

        val properties = new RaftProperties();

        RaftServerConfigKeys.setStorageDir(properties,
                Collections.singletonList(getStorageDir()));

        // RaftServerConfigKeys.setSleepDeviationThreshold();

        // TODO not working, thus can not be tested
//        RaftServerConfigKeys.Log.setUseMemory(properties,
//                true);

        val port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();

        GrpcConfigKeys.Server.setPort(properties, port);

        //create and start the Raft server
        return RaftServer.newBuilder()
                .setGroup(baseRaftGroup)
                .setProperties(properties)
                .setServerId(currentPeer.getId())
                .setStateMachineRegistry(raftGroupId -> {
                    if (raftGroupId.equals(BASE_GROUP_ID)) {
                        return clusterManagementStateMachine;
                    }
                    return null; // TODO error handling
                })
                .build();

        // TODO use raftServer.setConfiguration() for hot config changes ( new peers added etc. )
    }

    /**
     * Creates a basic client for this server
     * @return RaftClient
     */
    public RaftClient createClient() {
        val baseRaftGroup = getBaseRaftGroup();
        val properties = new RaftProperties();

        RaftClient.Builder builder = RaftClient.newBuilder()
            .setRaftGroup(baseRaftGroup)
            .setProperties(properties)
            .setClientRpc(
                new GrpcFactory(new Parameters())
                    .newRaftClientRpc(ClientId.randomId(), properties));
        return builder.build();
    }

    public void start() throws IOException, ExecutionException, InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        raftServer.start();

        isRunning = true;
        LOG.info("Cluster meta quorum server started");
    }

    private File getStorageDir() {
        return new File(raftConfig.getStoragePath() + "/cluster-management");
    }

    public void cleanUp() throws IOException {
        FileUtils.deleteFully(getStorageDir());
    }
}
