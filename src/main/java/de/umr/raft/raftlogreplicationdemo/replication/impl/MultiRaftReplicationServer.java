package de.umr.raft.raftlogreplicationdemo.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.replication.IReplicationServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.providers.StateMachineProvider;
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
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class MultiRaftReplicationServer implements IReplicationServer.Raft {

    protected static final Logger LOG =
        LoggerFactory.getLogger(MultiRaftReplicationServer.class);

    private final RaftServer raftServer;
    protected final RaftConfig raftConfig;

    // TODO or method to return this?
    //protected List<StateMachineProvider> stateMachineProviders = new ArrayList<>();
    // stateMachineProvider.createStateMachineInstance()
    // stateMachineProvider.createRaftGroup(peers)

    private final ClusterMetadataReplicationClient metaDataClient;
    // TODO private final ClusterManagementClient metaDataClient;

    protected abstract String getServerName();

    protected String getBaseRaftGroupName() {
        return getServerName() + ":base";
    }

    protected UUID getBaseRaftGroupUUID() {
        return UUID.nameUUIDFromBytes(getBaseRaftGroupName().getBytes(StandardCharsets.UTF_8));
    }

    protected List<StateMachineProvider> getDefaultStateMachineProviders() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return List.of();
    }

    protected List<StateMachineProvider> runTimeAddedStateMachineProviders = new ArrayList<>();

    /**
     * Returns list of state machine providers added at runtime (or retrieved from earlier snapshots)
     */
    private List<StateMachineProvider> getRuntimeStateMachineProviders() {
        // TODO can not use metadata client as this causes a deadlock since no leader server already started
        // TODO we NEED a seperate app logic server where raft groups can be spawned at runtime
        // TODO that server must wait for meta server to start
        // TODO otherwhise, there is no way to ask for metadata and thus running groups at all
        // TODO do so after docker build for AWS

//        val replicatedMetaDataMap = ReplicatedMetadataMap.ofRaftGroupScope(metaDataClient);
//        String raftGroupsString = null;
//        try {
//            raftGroupsString = replicatedMetaDataMap.get("groups");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println(raftGroupsString);

        //return List.of();
        return runTimeAddedStateMachineProviders;
    }

    /**
     * Returns list of state machine providers for default raft groups that are guaranteed to run at startup
     */
    protected List<StateMachineProvider> getStateMachineProviders() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return Stream.concat(getDefaultStateMachineProviders().stream(), this.getRuntimeStateMachineProviders().stream())
                .collect(Collectors.toList());
    }

    // TODO ClusterManagementClient metaDataClient;
    // TODO pass clients as components or create on the fly?

    @Autowired
    public MultiRaftReplicationServer(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException, NoSuchMethodException {
        this.raftConfig = raftConfig;
        this.metaDataClient = metaDataClient;
        this.raftServer = buildRaftServer();
    }

    /**
     * Returns a base raft group that does not correspond to any state machine.
     * It is only responsible to answer requests of the group management API
     * so we can add further groups after server is spawned.
     * Must be overridden in implementing classes to add peers
     */
    protected RaftGroup getBaseRaftGroup(UUID raftGroupUUID) {
        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
        return RaftGroup.valueOf(raftGroupId);
    }

    private RaftServer buildRaftServer() throws IOException {
        RaftGroup baseRaftGroup = getBaseRaftGroup(getBaseRaftGroupUUID());

        //find current peer object based on application parameter
        RaftPeer currentPeer =
                baseRaftGroup.getPeer(RaftPeerId.valueOf(raftConfig.getCurrentPeerId()));

        LOG.info("Current peer id: {}", raftConfig.getCurrentPeerId());
        LOG.info("Current peer address: {}", currentPeer.getAddress());

        val properties = new RaftProperties();

        RaftServerConfigKeys.setStorageDir(properties,
                Collections.singletonList(getStorageDir()));

        val port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();

        GrpcConfigKeys.Server.setPort(properties, port);

        //create and start the Raft server
        return RaftServer.newBuilder()
                .setGroup(baseRaftGroup)
                .setProperties(properties)
                .setServerId(currentPeer.getId())
                .setStateMachineRegistry(raftGroupId -> {
                    // TODO must be called on runtime as state machine for groups registered later are not recognized
                    // it is just called on startup
                    // TODO check how this can be called on runtime after a new group is added
                    // TODO Need seperate management and app logic server so mngmt server can handle even server restarts.
                    // not possible to obtain state machine providers before server startup as server is needed for that...
                    try {
                        val stateMachineProvider = getStateMachineProviders().stream().filter(provider ->
                                raftGroupId.equals(provider.getRaftGroupId(getServerName()))).findFirst().orElseThrow();
                        return stateMachineProvider.createStateMachineInstance();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return new BaseStateMachine(); // noop state machine
                    }
                })
                .build();

        // TODO use raftServer.setConfiguration() for hot config changes ( new peers added etc. )
    }

    /**
     * Creates a basic client for this server
     * @return RaftClient
     */
    public RaftClient createClient() {
        val baseRaftGroup = getBaseRaftGroup(getBaseRaftGroupUUID());
        val properties = new RaftProperties();

        RaftClient.Builder builder = RaftClient.newBuilder()
            .setRaftGroup(baseRaftGroup)
            // .setLeaderId(leaderId)
            .setProperties(properties)
            // .setParameters(parameters)
            // .setPrimaryDataStreamServer(primaryServer)
            //.setRetryPolicy(retryPolicy)
            .setClientRpc(
                new GrpcFactory(new Parameters())
                    .newRaftClientRpc(ClientId.randomId(), properties));
        return builder.build();
    }

    /**
     * Registers a new raft group if not already registered
     */
    private RaftGroupInfo registerNewRaftGroup(StateMachineProvider stateMachineProvider, boolean onlyOnThisNode) throws IOException, ExecutionException, InterruptedException {
        RaftGroupInfo raftGroupInfo;

        // TODO does not work reliably
        runTimeAddedStateMachineProviders.add(stateMachineProvider);

        try (final RaftClient client = createClient()) {
            val groupId = stateMachineProvider.getRaftGroupId(getServerName());
            List<String> peerIds = onlyOnThisNode
                    ? List.of(raftConfig.getCurrentPeerId())
                    : stateMachineProvider.getPeerIds();

            for (String peerId : peerIds) {
                val groupManagementApi = client.getGroupManagementApi(RaftPeerId.valueOf(peerId));
                // check if group already exists
                try {
                    // raftServer.getDivision(groupId);
                    GroupInfoReply groupInfo = groupManagementApi.info(groupId);
                    // TODO prevent StatusRuntimeException (deadline exceeded)
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        groupManagementApi.add(stateMachineProvider.createRaftGroup(getServerName()));
                        // TODO may need call config change?
                    } catch (AlreadyExistsException ex) {
                        // TODO why should we catch this? above attempt to get info should throw exception if not found!
                    }
                    // TODO check for success in clientReply
                }
            }
            // TODO only needed from current node from here
            GroupInfoReply groupInfoReply = client.getGroupManagementApi(RaftPeerId.valueOf(raftConfig.getCurrentPeerId())).info(groupId);

            raftGroupInfo = RaftGroupInfo.of(
                    groupInfoReply,
                    groupId.toString(),
                    stateMachineProvider.getStateMachineType(),
                    getServerName());

            broadcastRaftGroupInfo(
                    groupId.toString(),
                    groupId.getUuid(),
                    stateMachineProvider.getRaftGroupConfig().getDisplayName(getServerName()),
                    stateMachineProvider.getStateMachineType(),
                    stateMachineProvider.getPeerIds());
        }

        return raftGroupInfo;
    }

    public RaftGroupInfo registerNewRaftGroup(StateMachineProvider stateMachineProvider) throws IOException, ExecutionException, InterruptedException {
        return registerNewRaftGroup(stateMachineProvider, false);
    }

    private void registerRaftGroups() throws IOException, ExecutionException, InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // TODO only register if not already registered

        // TODO do we have to iterate over all the peers?
        // (of management group or the ones we want for the shard) to propagate groups properly...
        // for(RaftPeer peer : peers) {...

        for (val stateMachineProvider : getStateMachineProviders()) {
            registerNewRaftGroup(stateMachineProvider, true);
        }
    }

    public void start() throws IOException, ExecutionException, InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        LOG.info("Raft server started");
        raftServer.start();

        // register default raft groups and those found in snapshots
        registerRaftGroups();

        val baseGroup = getBaseRaftGroup(getBaseRaftGroupUUID());

        broadcastRaftGroupInfo(
                baseGroup.getGroupId().toString(),
                getBaseRaftGroupUUID(),
                getBaseRaftGroupName(),
                "base",
                baseGroup.getPeers().stream().map(raftPeer -> raftPeer.getId().toString()).collect(Collectors.toList()));
    }

    private File getStorageDir() {
        return new File(raftConfig.getStoragePath() + "/" + getServerName());
    }

    public void cleanUp() throws IOException {
        FileUtils.deleteFully(getStorageDir());
    }

    // TODO not only register in map of current peer, but in all peers
    private void broadcastRaftGroupInfo(String groupId, UUID groupUUID, String groupName, String stateMachineType, List<String> nodeIds) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val replicatedMetaDataMap = ReplicatedMetadataMap.ofRaftGroupScope(metaDataClient);

        String raftGroupsString;
        try {
            raftGroupsString = replicatedMetaDataMap.get("groups");
        } catch (NoSuchElementException e) {
            raftGroupsString = null;
        }

        if (raftGroupsString == null) raftGroupsString = "";

        val raftGroups = raftGroupsString.split(";");
        val raftGroupsSet = new HashSet<String>(Arrays.asList(raftGroups));
        raftGroupsSet.add(groupId);
        raftGroupsString = String.join(";", raftGroupsSet);

        replicatedMetaDataMap.put("groups", raftGroupsString);

        // add more info about raft group here
        replicatedMetaDataMap.put(groupId + "-UUID", groupUUID.toString());
        replicatedMetaDataMap.put(groupId + "-name", groupName);
        replicatedMetaDataMap.put(groupId + "-server", getServerName());
        replicatedMetaDataMap.put(groupId + "-sm-class", stateMachineType);
        replicatedMetaDataMap.put(groupId + "-sm-name", stateMachineType);
        replicatedMetaDataMap.put(groupId + "-nodes", String.join(";", nodeIds));
    }

}
