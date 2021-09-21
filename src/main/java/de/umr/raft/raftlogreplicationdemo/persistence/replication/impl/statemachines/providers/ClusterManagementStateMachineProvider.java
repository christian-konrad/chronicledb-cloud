package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.providers;

import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.ClusterManagementStateMachine;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.ClusterMetadataStateMachine;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class ClusterManagementStateMachineProvider extends StateMachineProvider<ClusterManagementStateMachine> {

    public ClusterManagementStateMachineProvider(RaftGroupConfig raftGroupConfig) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        super(ClusterManagementStateMachine.class, raftGroupConfig);
    }

    public static ClusterManagementStateMachineProvider of(String groupName, List<RaftPeer> peers) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return new ClusterManagementStateMachineProvider(RaftGroupConfig.of(peers, groupName));
    }
}
