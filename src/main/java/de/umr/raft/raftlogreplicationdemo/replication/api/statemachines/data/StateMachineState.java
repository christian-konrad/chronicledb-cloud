package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.data;

import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;

import java.io.IOException;
import java.io.ObjectInputStream;

public interface StateMachineState {
    void clear();

    void loadFrom(ObjectInputStream in) throws IOException, ClassNotFoundException;

    Object createSnapshot();

    void initState(Object ...args) throws IOException;

    enum Phase {
        UNINITIALIZED,
        INITIALIZED
    }
}
