package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.clustermanagement.ClusterManagementOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementServer;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ClusterManagementClient extends RaftReplicationClient<ClusterManagementOperationMessage> {

    public static ClusterManagementOperationMessage createRegisterPartitionOperationMessage(String stateMachineClassname, String partitionName, int replicationFactor) {
        return ClusterManagementOperationMessage.Factory.createRegisterPartitionOperationMessage(stateMachineClassname, partitionName, replicationFactor);
    }

    public static ClusterManagementOperationMessage createProvisionPartitionOperationMessage(String stateMachineClassname, String partitionName) {
        return ClusterManagementOperationMessage.Factory.createProvisionPartitionOperationMessage(stateMachineClassname, partitionName);
    }

    public static ClusterManagementOperationMessage createAcknowledgePartitionRegistrationOperationMessage(String stateMachineClassname, String partitionName) {
        return ClusterManagementOperationMessage.Factory.createAcknowledgePartitionRegistrationOperationMessage(stateMachineClassname, partitionName);
    }

    public static ClusterManagementOperationMessage createDetachPartitionOperationMessage(String stateMachineClassname, String partitionName) {
        return ClusterManagementOperationMessage.Factory.createDetachPartitionOperationMessage(stateMachineClassname, partitionName);
    }

    public static ClusterManagementOperationMessage createListPartitionsOperationMessage(String stateMachineClassname) {
        return ClusterManagementOperationMessage.Factory.createListPartitionsOperationMessage(stateMachineClassname);
    }

    public static ClusterManagementOperationMessage createHeartbeatOperationMessage(RaftPeer raftPeer) {
        return ClusterManagementOperationMessage.Factory.createHeartbeatOperationMessage(raftPeer);
    }

    public static ClusterManagementOperationMessage createGetHealthOperationMessage() {
        return ClusterManagementOperationMessage.Factory.createGetHealthOperationMessage();
    }

    public static ClusterManagementOperationMessage getStateMachineForRaftGroupOperationMessage(RaftGroupId raftGroupId) {
        return ClusterManagementOperationMessage.Factory.getStateMachineForRaftGroupOperationMessage(raftGroupId);
    }

    @Override
    protected UUID getRaftGroupUUID() {
        return ClusterManagementServer.BASE_GROUP_UUID;
    }

    @Override
    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        return raftConfig.getManagementRaftGroup(getRaftGroupUUID());
    }

    @Autowired
    public ClusterManagementClient(RaftConfig raftConfig) {
        super(raftConfig);
    }
}
