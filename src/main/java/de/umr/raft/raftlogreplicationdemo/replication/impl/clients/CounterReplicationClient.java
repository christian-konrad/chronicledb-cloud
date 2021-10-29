package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import org.springframework.beans.factory.annotation.Autowired;

public class CounterReplicationClient extends PartitionedRaftReplicationClient<CounterOperationMessage> {

    public String getCounterId() {
        return getPartitionId();
    }

    public static CounterOperationMessage createIncrementOperationMessage() {
        return CounterOperationMessage.Factory.createIncrementOperationMessage();
    }

    public static CounterOperationMessage createGetOperationMessage() {
        return CounterOperationMessage.Factory.createGetOperationMessage();
    }

    @Autowired
    public CounterReplicationClient(RaftConfig raftConfig, String counterId) {
        super(raftConfig, counterId);
    }
}
