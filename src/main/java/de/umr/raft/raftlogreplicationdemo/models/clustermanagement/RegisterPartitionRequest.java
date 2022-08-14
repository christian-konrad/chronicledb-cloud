package de.umr.raft.raftlogreplicationdemo.models.clustermanagement;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import java.util.Optional;

@RequiredArgsConstructor
public class RegisterPartitionRequest {
    @Getter @NonNull final String stateMachineClassName;
    @Getter @NonNull final String partitionName;
    final Optional<Integer> replicationFactor;

    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    public Class<? extends BaseStateMachine> getStateMachineClass() throws ClassNotFoundException {
        return Class.forName(stateMachineClassName).asSubclass(BaseStateMachine.class);
    }

//    public RegisterPartitionRequest(String stateMachineClassName, String partitionName) {
//        this.stateMachineClassName = stateMachineClassName;
//        this.partitionName = partitionName;
//        this.replicationFactor = DEFAULT_REPLICATION_FACTOR;
//    }
//
//    public RegisterPartitionRequest(String stateMachineClassName, String partitionName, int replicationFactor) {
//        this.stateMachineClassName = stateMachineClassName;
//        this.partitionName = partitionName;
//        this.replicationFactor = replicationFactor;
//    }

    public int getReplicationFactor() {
        return replicationFactor.orElse(3);
    }
}
