package de.umr.raft.raftlogreplicationdemo.models.clustermanagement;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

@RequiredArgsConstructor
public class DetachPartitionRequest {
    @Getter @NonNull final String stateMachineClassName;
    @Getter @NonNull final String partitionName;

    public Class<? extends BaseStateMachine> getStateMachineClass() throws ClassNotFoundException {
        return Class.forName(stateMachineClassName).asSubclass(BaseStateMachine.class);
    }
}
