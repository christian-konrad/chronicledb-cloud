package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement;

// TODO other names, like keeper, orchestrator... any?

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.ClusterHealth;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.NodeHealth;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.clustermanagement.ClusterManagementOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ApplicationLogicServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterManagementClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.ClusterMetadataStateMachine;
import de.umr.raft.raftlogreplicationdemo.util.FutureUtil;
import lombok.val;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Facade to manage this cluster. This component allows you to
 * - register new partitions for state machine instances
 * - retrieve information on currently registered partitions
 * - add new peers to the cluster
 * - change the replication factor of a partition
 * and more.
 */
@Component
public class ClusterManager {

    private final ClusterManagementClient clusterManagementClient;
    private final ClusterMetadataReplicationClient clusterMetadataClient;
    private final RaftConfig raftConfig;

    private final Map<RaftGroupId, String> stateMachineGroupMapping = new HashMap<>();

    Logger LOG = LoggerFactory.getLogger(ClusterManager.class);

//    private final Daemon heartbeatEmitter;

    @Autowired
    public ClusterManager(ClusterManagementClient clusterManagementClient, ClusterMetadataReplicationClient clusterMetadataClient, RaftConfig raftConfig) throws IOException, ExecutionException, InterruptedException {
        LOG.info("Instantiating the Cluster Manager");

        this.clusterManagementClient = clusterManagementClient;
        this.clusterMetadataClient = clusterMetadataClient;
        this.raftConfig = raftConfig;

//        heartbeatEmitter = new Daemon(new HeartbeatEmitter(), HeartbeatEmitter.DAEMON_NAME);
//        heartbeatEmitter.start();
    }

    private ClusterManagementOperationResultProto sendAndExecuteOperationMessage(ClusterManagementOperationMessage operationMessage) {
        ClusterManagementOperationResultProto resultProto;

        try {
            resultProto = clusterManagementClient.sendAndExecuteOperationMessage(
                    operationMessage,
                    ClusterManagementOperationResultProto.parser());
        } catch (ExecutionException | InterruptedException | InvalidProtocolBufferException e) {
            e.printStackTrace();
            // TODO better, custom expection; like StateMachineMessageExecutionException
            throw new UnsupportedOperationException();
        }

        if (!resultProto.getStatus().equals(OperationResultStatus.OK)
                || !resultProto.getOperationType().equals(operationMessage.getClusterManagementOperation().getOperationType())) {
            // TODO better, custom expection; like StateMachineMessageExecutionException
            throw new UnsupportedOperationException();
        }

        return resultProto;
    }

    // public PartitionInfo registerPartition(Class<? extends BaseStateMachine> stateMachineClass
    public <T extends BaseStateMachine> PartitionInfo registerPartition(Class<T> stateMachineClass, String partitionName, int replicationFactor) {

        var stateMachineClassName = stateMachineClass.getCanonicalName();

        var operationMessage =
                ClusterManagementClient.createRegisterPartitionOperationMessage(
                        stateMachineClassName,
                        partitionName,
                        replicationFactor
                );

        ClusterManagementOperationResultProto resultProto = null;

        resultProto = sendAndExecuteOperationMessage(operationMessage);

        // unpack proto
        // TODO check if it contains response
        var responseProto = resultProto.getResponse().getRegisterPartitionResponse();
        var partitionInfoProto = responseProto.getPartitionInfo();

        // in async thread, don't let client wait
        FutureUtil.wrapInCompletableFuture(() -> {
            requestProvisioning(stateMachineClassName, partitionName);
            acknowledgeProvisioning(stateMachineClassName, partitionName);

            var raftGroupProto = partitionInfoProto.getRaftGroup();

            broadcastRaftGroupInfo(
                    ApplicationLogicServer.SERVER_NAME,
                    RaftGroupId.valueOf(raftGroupProto.getId()).toString(),
                    UUID.fromString(raftGroupProto.getUuid()), //META_GROUP_UUID,
                    raftGroupProto.getName(), //META_GROUP_NAME,
                    stateMachineClass.getCanonicalName(),
                    raftGroupProto.getPeersList().stream().map(raftPeer -> RaftPeerId.valueOf(raftPeer.getId()).toString()).collect(Collectors.toList()));
                    //raftConfig.getManagementPeersList().stream().map(raftPeer -> raftPeer.getId().toString()).collect(Collectors.toList()));
            return null;
        });

        return PartitionInfo.of(partitionInfoProto, stateMachineClassName);
    }

    // called as query message only on leader node to prevent all nodes to provision the same partition
    private ClusterManagementOperationResultProto requestProvisioning(String stateMachineClassname, String partitionName) {
        LOG.info("Requesting the provisioning of partitions");

        var operationMessage =
                ClusterManagementClient.createProvisionPartitionOperationMessage(
                        stateMachineClassname,
                        partitionName
                );

        var result = sendAndExecuteOperationMessage(operationMessage);

        LOG.info("Provisioning request done");

        return result;
    }

    private ClusterManagementOperationResultProto acknowledgeProvisioning(String stateMachineClassname, String partitionName) {
        LOG.info("Acknowledging the provisioning of partitions");

        var operationMessage =
                ClusterManagementClient.createAcknowledgePartitionRegistrationOperationMessage(
                        stateMachineClassname,
                        partitionName
                );

        LOG.info("Before send ack request");

        var result = sendAndExecuteOperationMessage(operationMessage);

        LOG.info("Acknowledging request done");

        return result;
    }

    public <T extends BaseStateMachine> PartitionInfo detachPartition(Class<T> stateMachineClass, String partitionName) {

        var stateMachineClassName = stateMachineClass.getCanonicalName();

        var operationMessage =
                ClusterManagementClient.createDetachPartitionOperationMessage(
                        stateMachineClassName,
                        partitionName
                );

        ClusterManagementOperationResultProto resultProto = null;

        resultProto = sendAndExecuteOperationMessage(operationMessage);

        // unpack proto
        // TODO check if it contains response
        var responseProto = resultProto.getResponse().getRegisterPartitionResponse();
        var partitionInfoProto = responseProto.getPartitionInfo();

        return PartitionInfo.of(partitionInfoProto, stateMachineClassName);
    }

    private List<PartitionInfo> listPartitions(String stateMachineClassname) {
        var operationMessage =
                ClusterManagementClient.createListPartitionsOperationMessage(stateMachineClassname);

        ClusterManagementOperationResultProto resultProto = null;

        resultProto = sendAndExecuteOperationMessage(operationMessage);

        // unpack proto
        // TODO check if it contains response
        var responseProto = resultProto.getResponse().getListPartitionsResponse();
        var partitionInfoProtos = responseProto.getPartitionInfoList();

        return partitionInfoProtos.stream().map(PartitionInfo::of).collect(Collectors.toList());
    }

    private static final String ALL_STATE_MACHINES = "ALL";

    public List<PartitionInfo> listPartitions() {
        return listPartitions(ALL_STATE_MACHINES);
    }

    public <T extends BaseStateMachine> List<PartitionInfo> listPartitions(Class<T> stateMachineClass) {
        var stateMachineClassName = stateMachineClass.getCanonicalName();
        return listPartitions(stateMachineClassName);
    }

    public void updateStateMachineRaftGroupCache() {

    }

    private Class<? extends BaseStateMachine> getStateMachineForRaftGroup(RaftGroupId raftGroupId) throws ClassNotFoundException {
        LOG.info("getStateMachineForRaftGroup {}", raftGroupId);

        var operationMessage =
                ClusterManagementClient.getStateMachineForRaftGroupOperationMessage(raftGroupId);

        ClusterManagementOperationResultProto resultProto = null;

        resultProto = sendAndExecuteOperationMessage(operationMessage);

        var responseProto = resultProto.getResponse().getGetStateMachineForRaftGroupResponse();
        var stateMachineClassName = responseProto.getStateMachineClassname();

        return Class.forName(stateMachineClassName).asSubclass(BaseStateMachine.class);
    }

    // TODO must cache this. Can't access this while clustermanager is already busy registering a partition
    public Class<? extends BaseStateMachine> getStateMachineForRaftGroupCached(RaftGroupId raftGroupId) throws ClassNotFoundException {
        LOG.info("getStateMachineForRaftGroup {}", raftGroupId);

        /*var operationMessage =
                ClusterManagementClient.getStateMachineForRaftGroupOperationMessage(raftGroupId);

        ClusterManagementOperationResultProto resultProto = null;

        resultProto = sendAndExecuteOperationMessage(operationMessage);

        var responseProto = resultProto.getResponse().getGetStateMachineForRaftGroupResponse();
        var stateMachineClassName = responseProto.getStateMachineClassname();*/

        LOG.info("stateMachineGroupMapping {}", stateMachineGroupMapping);
        var stateMachineClassname = stateMachineGroupMapping.get(raftGroupId);
        LOG.info("stateMachineClassname {}", stateMachineClassname);

        if (stateMachineClassname == null || stateMachineClassname.isEmpty()) return getStateMachineForRaftGroup(raftGroupId);

        return Class.forName(stateMachineClassname).asSubclass(BaseStateMachine.class);
    }

//    public void detachPartition() {
//
//    }

    public void sendHeartbeat(RaftPeer thisPeer) {
        // LOG.info("Sending heartbeat from {}", thisPeer);
        var operationMessage =
                ClusterManagementClient.createHeartbeatOperationMessage(thisPeer);

        sendAndExecuteOperationMessage(operationMessage);
    }

    public ClusterHealth getClusterHealth() {
        var operationMessage =
                ClusterManagementClient.createGetHealthOperationMessage();

        sendAndExecuteOperationMessage(operationMessage);

        ClusterManagementOperationResultProto resultProto = null;

        resultProto = sendAndExecuteOperationMessage(operationMessage);

        // unpack proto
        // TODO check if it contains response
        var responseProto = resultProto.getResponse().getClusterHealthResponse();
        var isHealthy = responseProto.getIsHealthy();
        var nodeHealths = responseProto.getNodeInfoList();

        return ClusterHealth.of(isHealthy, nodeHealths.stream().map(nodeHealthProto -> NodeHealth.of(nodeHealthProto)).collect(Collectors.toList()));
    }

    /**
     * Returns the base raft group responsible for handling the cluster management
     */
    private RaftGroup getBaseRaftGroup() {
        return raftConfig.getManagementRaftGroup(ClusterManagementServer.BASE_GROUP_UUID);
    }

    // cheap solution, but not efficient
    public void setStateMachineGroupMappingCache(Map<RaftGroupId, String> stateMachineGroupMapping) {
        LOG.info("Updating state machine group mapping cache with {}", stateMachineGroupMapping);

        this.stateMachineGroupMapping.clear();
        this.stateMachineGroupMapping.putAll(stateMachineGroupMapping);
    }

    public void broadcastRaftGroupInfo(String servername, String groupId, UUID groupUUID, String groupName, String stateMachineType, List<String> nodeIds) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val groupRegistryDataMap = ReplicatedMetadataMap.ofRaftGroupRegistry(clusterMetadataClient);
        val groupMetaDataMap = ReplicatedMetadataMap.ofRaftGroupScope(groupId, clusterMetadataClient);

        String raftGroupsString;
        try {
            raftGroupsString = groupRegistryDataMap.get("groups");
        } catch (NoSuchElementException e) {
            raftGroupsString = null;
        }

        if (raftGroupsString == null) raftGroupsString = "";

        val raftGroups = raftGroupsString.split(";");
        val raftGroupsSet = new HashSet<String>(Arrays.asList(raftGroups));
        raftGroupsSet.add(groupId);
        raftGroupsString = String.join(";", raftGroupsSet);

        groupRegistryDataMap.put("groups", raftGroupsString);

        // add more info about raft group here
        groupRegistryDataMap.put(groupId + "-UUID", groupUUID.toString());
        groupMetaDataMap.put("UUID", groupUUID.toString());
        groupMetaDataMap.put("name", groupName);
        groupMetaDataMap.put("server", servername);
        groupMetaDataMap.put("sm-class", stateMachineType);
        groupMetaDataMap.put("sm-name", stateMachineType);
        groupMetaDataMap.put("nodes", String.join(";", nodeIds));
    }

    // returns only the division for the queried node...
/*    public RaftServer.Division getDivision(String serverName, RaftGroupId groupId) throws IOException {
        switch (serverName) {
            case ApplicationLogicServer.SERVER_NAME:
                return applicationLogicServer.getRaftServer().getDivision(groupId);
            case ClusterManagementServer.SERVER_NAME:
                return clusterManagementServer.getRaftServer().getDivision(groupId);
        };
        return null;
    }*/

}
