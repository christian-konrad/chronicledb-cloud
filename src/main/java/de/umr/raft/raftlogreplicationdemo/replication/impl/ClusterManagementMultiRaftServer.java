package de.umr.raft.raftlogreplicationdemo.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.providers.*;
import lombok.val;
import org.apache.ratis.protocol.RaftGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;

//@Component
public class ClusterManagementMultiRaftServer extends MultiRaftReplicationServer {

    // TODO AppLogicServer

    public final static String SERVER_NAME = "cluster-management";

    @Override
    protected String getServerName() {
        return SERVER_NAME;
    }

    @Override
    protected RaftGroup getBaseRaftGroup(UUID raftGroupUUID) {
        return raftConfig.getManagementRaftGroup(raftGroupUUID);
    }

    @Override
    protected List<StateMachineProvider> getDefaultStateMachineProviders() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        val peers = raftConfig.getManagementPeersList();
        // TODO base state machine must be management machine ?
        return List.of(
                ClusterMetaDataStateMachineProvider.of("metadata", peers),
                ClusterManagementStateMachineProvider.of("keeper", peers),
                CounterStateMachineProvider.of("counter", peers),
                EventStoreStateMachineProvider.of("demo-event-store", peers));
    }

//    @Autowired
    public ClusterManagementMultiRaftServer(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super(raftConfig, metaDataClient);
    }
}
