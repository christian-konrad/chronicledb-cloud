package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.providers;

import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.CounterStateMachine;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class CounterStateMachineProvider extends StateMachineProvider<CounterStateMachine> {

    public CounterStateMachineProvider(StateMachineProvider.RaftGroupConfig raftGroupConfig) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        super(CounterStateMachine.class, raftGroupConfig);
    }

    public static CounterStateMachineProvider of(String groupName, List<RaftPeer> peers) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return new CounterStateMachineProvider(CounterStateMachineProvider.RaftGroupConfig.of(peers, groupName));
    }
}
