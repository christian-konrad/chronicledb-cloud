package de.umr.raft.raftlogreplicationdemo.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.IReplicationServer;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.IdleStateMachine;
import lombok.Getter;
import lombok.val;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.Daemon;
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class ApplicationLogicServer implements IReplicationServer.Raft {

    // TODO unify common code of both servers in abstract class

    private static final Logger LOG =
        LoggerFactory.getLogger(ApplicationLogicServer.class);

    @Getter private RaftServer raftServer;
    private final RaftConfig raftConfig;
    private final ClusterManager clusterManager;
    private final ClusterMetadataReplicationClient clusterMetadataClient;

    private final Daemon heartbeatEmitter;

    @Getter private boolean isRunning = false;

    public static final String SERVER_NAME = "application-server";

    public static final String BASE_GROUP_NAME = SERVER_NAME + ":base";
    public static final UUID BASE_GROUP_UUID = UUID.nameUUIDFromBytes(BASE_GROUP_NAME.getBytes(StandardCharsets.UTF_8));
    public static final RaftGroupId BASE_GROUP_ID = RaftGroupId.valueOf(BASE_GROUP_UUID);

    @Autowired
    public ApplicationLogicServer(RaftConfig raftConfig, ClusterManager clusterManager, ClusterMetadataReplicationClient clusterMetadataClient) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException, NoSuchMethodException {
        this.raftConfig = raftConfig;
        this.clusterManager = clusterManager;
        this.clusterMetadataClient = clusterMetadataClient;

        heartbeatEmitter = new Daemon(new HeartbeatEmitter(), HeartbeatEmitter.DAEMON_NAME);
    }

    /**
     * Returns the base raft group need to initialize the server and responsible for keeping the server alive
     */
    private RaftGroup getBaseRaftGroup() {
        var peer = RaftPeer.newBuilder()
                .setId(raftConfig.getCurrentPeerId())
                .setAddress(raftConfig.getReplicationAddress())
                .build();
        return RaftGroup.valueOf(BASE_GROUP_ID, peer);
    }

    private RaftServer buildRaftServer() throws IOException {
        RaftGroup baseRaftGroup = getBaseRaftGroup();

        // find current peer object based on application parameter
        RaftPeer currentPeer =
                baseRaftGroup.getPeer(RaftPeerId.valueOf(raftConfig.getCurrentPeerId()));

        LOG.info("[App Server] Current peer id: {}", raftConfig.getCurrentPeerId());
        LOG.info("[App Server] Current peer address: {}", currentPeer.getAddress());

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
                        // we need to spawn at least a stub state machine on server startup,
                        // this is a ratis limitation
                        return new IdleStateMachine();
                    }
                    // TODO how to instantiate the right state machine here? How to get that info?
                    // -> May need to poll ClusterManager for group->stateMachine mapping
                    try {
                        var stateMachineClass = clusterManager.getStateMachineForRaftGroupCached(raftGroupId);
                        LOG.info("Registering state machine {}", stateMachineClass.getSimpleName());
                        return stateMachineClass.getConstructor().newInstance();
                    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        e.printStackTrace();
                        // TODO something more meaningful
                        return new IdleStateMachine();
                    }
                })
                .build();

        // TODO start daemon to send heartbeats to management
    }

    // TODO won't work with baseRaftGroup
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
        raftServer = buildRaftServer();
        raftServer.start();

        isRunning = true;

        heartbeatEmitter.start();
        LOG.info("Application server started");
    }

    private File getStorageDir() {
        return new File(raftConfig.getStoragePath() + "/" + SERVER_NAME);
    }

    public void cleanUp() throws IOException {
        FileUtils.deleteFully(getStorageDir());
    }

    private RaftPeer getThisPeer() {
        RaftGroup baseRaftGroup = getBaseRaftGroup();

        // find current peer object based on application parameter
        return baseRaftGroup.getPeer(RaftPeerId.valueOf(raftConfig.getCurrentPeerId()));
    }

    public List<RaftServer.Division> getAllDivisions() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return clusterManager.listPartitions().stream().map(PartitionInfo::getRaftGroup).map(raftGroup -> {
            RaftGroupId groupId = raftGroup.getGroupId();
            try {
                return getRaftServer().getDivision(groupId);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private void broadcastDivisionInfo() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ExecutionException, InterruptedException {
        List<RaftServer.Division> divisions = getAllDivisions();
        for (RaftServer.Division division : divisions) {
            val divisionMetaData = ReplicatedMetadataMap.ofDivision(division.getMemberId().toString(), clusterMetadataClient);
            val currentTerm = division.getInfo().getCurrentTerm();
            val lastAppliedIndex = division.getInfo().getLastAppliedIndex();
            val role = division.getInfo().getCurrentRole();
            val isAlive = division.getInfo().isAlive();

            // val serverMetrics = division.getRaftServerMetrics();
            // val logMetrics = division.getRaftLog().getRaftLogMetrics().toString();

            divisionMetaData.putAsync("currentTerm", String.valueOf(currentTerm));
            divisionMetaData.putAsync("lastAppliedIndex", String.valueOf(lastAppliedIndex));
            divisionMetaData.putAsync("role", role.name());
            divisionMetaData.putAsync("isAlive", String.valueOf(isAlive));

            // divisionMetaData.putAsync("serverMetrics", serverMetrics.toString());
            // divisionMetaData.putAsync("logMetrics", String.valueOf(logMetrics));
        }
    }

    private class HeartbeatEmitter implements Runnable {

        static final String DAEMON_NAME = "peer-heartbeat-emitter";
        final RaftPeer thisPeer;

        public HeartbeatEmitter() {
            thisPeer = getThisPeer();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(raftConfig.getHeartbeatInterval());
                    clusterManager.sendHeartbeat(thisPeer);
                    broadcastDivisionInfo();
                } catch (Exception e) {
                    LOG.error("Exception while sending peer heartbeat", e);
                }
            }
        }
    }
}
