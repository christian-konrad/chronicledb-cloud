package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement;

// TODO other names, like keeper, orchestrator... any?

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.clustermanagement.ClusterManagementOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterManagementClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedChronicleEngine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.ExecutableMessageStateMachine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.providers.StateMachineProvider;
import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.SystemInfoService;
import lombok.val;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
    private final RaftConfig raftConfig;

    Logger LOG = LoggerFactory.getLogger(ClusterManager.class);

    @Autowired
    public ClusterManager(ClusterManagementClient clusterManagementClient, RaftConfig raftConfig) throws IOException, ExecutionException, InterruptedException {
        LOG.info("Instantiating the Cluster Manager");

        this.clusterManagementClient = clusterManagementClient;
        this.raftConfig = raftConfig;
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

        return PartitionInfo.of(partitionInfoProto, stateMachineClassName);
    }

//    public void detachPartition() {
//
//    }
//
//    public List<PartitionInfo> listPartitions() {
//
//    }

    // TODO use in heartbeat runner, but first test with controller
    public void sendHeartbeat(RaftPeer thisPeer) {
        var operationMessage =
                ClusterManagementClient.createHeartbeatOperationMessage(thisPeer);

        sendAndExecuteOperationMessage(operationMessage);
    }

    // TODO methods to retrieve cluster health etc (like previously in metadata service)
}
