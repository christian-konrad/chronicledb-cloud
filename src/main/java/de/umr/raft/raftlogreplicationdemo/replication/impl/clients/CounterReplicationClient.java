package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import org.apache.ratis.protocol.RaftGroup;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

public class CounterReplicationClient extends PartitionedRaftReplicationClient<CounterOperationMessage> {

    // client instance registry
    private static Map<String, CounterReplicationClient> INSTANCES;

    public static CounterReplicationClient of(RaftConfig raftConfig, String partitionId, RaftGroup raftGroup) {
        if (INSTANCES == null) {
            INSTANCES = new HashMap<>();
        }

        if (!INSTANCES.containsKey(partitionId)) {
            CounterReplicationClient client =
                    new CounterReplicationClient(raftConfig, partitionId, raftGroup);
            INSTANCES.put(partitionId, client);
        }

        return INSTANCES.get(partitionId);
    }

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
    public CounterReplicationClient(RaftConfig raftConfig, String counterId, RaftGroup raftGroup) {
        super(raftConfig, counterId, raftGroup);
    }
}
