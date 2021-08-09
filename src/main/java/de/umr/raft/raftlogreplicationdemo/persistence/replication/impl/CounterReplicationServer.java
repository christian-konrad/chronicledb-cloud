package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.CounterStateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

public class CounterReplicationServer extends RaftReplicationServer<CounterStateMachine> {

    @Override
    protected UUID getRaftGroupUUID() {
        return UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");
    }

    public CounterReplicationServer(RaftConfig raftConfig) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super(raftConfig, CounterStateMachine.class);
    }
}
