package de.umr.raft.raftlogreplicationdemo.persistence.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.CounterReplicationServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;

@Component
public class CounterReplicationServerStartupRunner extends RaftReplicationServerStartupRunner<CounterReplicationServer> {

    @Autowired
    public CounterReplicationServerStartupRunner(RaftConfig raftConfig) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        super(raftConfig, CounterReplicationServer.class);
    }
}
