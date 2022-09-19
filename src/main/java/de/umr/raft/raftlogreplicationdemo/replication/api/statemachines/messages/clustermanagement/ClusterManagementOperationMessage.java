package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement.ClusterManagementOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class ClusterManagementOperationMessage implements ExecutableMessage<ClusterStateManager, ClusterManagementOperationResultProto> {

    @Getter private final ClusterManagementOperationProto clusterManagementOperation;

    public static ClusterManagementOperationMessage of(ByteString bytes) throws InvalidProtocolBufferException {
        return ClusterManagementOperationMessage.of(ClusterManagementOperationProto.parseFrom(bytes));
    }

    @Override
    public ByteString getContent() {
        return clusterManagementOperation.toByteString();
    }

    // TODO may implement isValid and check message schema

    @Override
    public boolean isTransactionMessage() {
        switch (clusterManagementOperation.getOperationType()) {
            case REGISTER_PARTITION:
            case DETACH_PARTITION:
            case ACKNOWLEDGE_PARTITION_REGISTRATION:
            case HEARTBEAT:
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return clusterManagementOperation.toString();
    }

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> apply(ClusterStateManager clusterStateManager) {
        return ClusterManagementOperationExecutor.of(clusterManagementOperation).apply(clusterStateManager);
    }

    @Override
    public CompletableFuture<ClusterManagementOperationResultProto> cancel() {
        return ClusterManagementOperationExecutor.of(clusterManagementOperation).cancel();
    }

    public static class Factory {
        public static ClusterManagementOperationMessage createRegisterPartitionOperationMessage(String stateMachineClassname, String partitionName, int replicationFactor) {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.REGISTER_PARTITION)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setRegisterPartitionRequest(RegisterPartitionRequestProto.newBuilder()
                                    .setStatemachineClassname(stateMachineClassname)
                                    .setPartitionName(partitionName)
                                    .setReplicationFactor(replicationFactor)
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage createProvisionPartitionOperationMessage(String stateMachineClassname, String partitionName) {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.INSTANTIATE_PARTITION)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setInstantiatePartitionRequest(InstantiatePartitionRequestProto.newBuilder()
                                    .setStatemachineClassname(stateMachineClassname)
                                    .setPartitionName(partitionName)
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage createAcknowledgePartitionRegistrationOperationMessage(String stateMachineClassname, String partitionName) {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.ACKNOWLEDGE_PARTITION_REGISTRATION)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setAcknowledgePartitionRegistrationRequest(AcknowledgePartitionRegistrationProto.newBuilder()
                                    .setStatemachineClassname(stateMachineClassname)
                                    .setPartitionName(partitionName)
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage createDetachPartitionOperationMessage(String stateMachineClassname, String partitionName) {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.DETACH_PARTITION)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setDetachPartitionRequest(DetachPartitionRequestProto.newBuilder()
                                    .setPartitionName(partitionName)
                                    .setStatemachineClassname(stateMachineClassname)
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage createListPartitionsOperationMessage(String stateMachineClassname) {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.LIST_PARTITIONS)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setListPartitionsRequest(ListPartitionsRequestProto.newBuilder()
                                    .setStatemachineClassname(stateMachineClassname)
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage createHeartbeatOperationMessage(RaftPeer raftPeer) {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.HEARTBEAT)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setHeartbeatRequestProto(HeartbeatRequestProto.newBuilder()
                                    .setPeer(RaftPeerProto
                                            .newBuilder()
                                            .setId(raftPeer.getId().toByteString())
                                            .setAddress(raftPeer.getAddress())
                                            .build())
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage createGetHealthOperationMessage() {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.GET_CLUSTER_HEALTH)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setGetClusterHealthRequestProto(GetClusterHealthRequestProto.newBuilder()
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage getStateMachineForRaftGroupOperationMessage(RaftGroupId raftGroupId) {
            val metadataOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.GET_STATEMACHINE_FOR_RAFT_GROUP)
                    .setRequest(ClusterManagementRequestProto.newBuilder()
                            .setGetStateMachineForRaftGroupRequest(GetStateMachineForRaftGroupProto.newBuilder()
                                    .setRaftGroupId(raftGroupId.toByteString())
                                    .build())
                            .build())
                    .build();

            return ClusterManagementOperationMessage.of(metadataOperation);
        }

        public static ClusterManagementOperationMessage createNullOperationMessage() {
            val clusterManagementOperation = ClusterManagementOperationProto.newBuilder()
                    .setOperationType(ClusterManagementOperationType.NULL)
                    .build();

            return ClusterManagementOperationMessage.of(clusterManagementOperation);
        }
    }
}
