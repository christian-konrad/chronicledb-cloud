package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CounterReplicationClient extends RaftReplicationClient {

    @Override
    protected UUID getRaftGroupUUID() {
        return UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");
    }

    @Autowired
    public CounterReplicationClient(RaftConfig raftConfig) {
        super(raftConfig);
    }
}
