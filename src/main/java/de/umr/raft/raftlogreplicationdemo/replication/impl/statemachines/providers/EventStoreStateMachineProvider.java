package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.providers;

import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.EventStoreStateMachine;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class EventStoreStateMachineProvider extends StateMachineProvider<EventStoreStateMachine> {

    public EventStoreStateMachineProvider(RaftGroupConfig raftGroupConfig) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        super(EventStoreStateMachine.class, raftGroupConfig);
    }

    public static EventStoreStateMachineProvider of(String groupName, List<RaftPeer> peers) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return new EventStoreStateMachineProvider(RaftGroupConfig.of(peers, groupName));
    }
}
