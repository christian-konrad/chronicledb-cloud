package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.ClusterManagementStateMachine;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.ClusterMetadataStateMachine;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.CounterStateMachine;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.providers.ClusterManagementStateMachineProvider;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.providers.ClusterMetaDataStateMachineProvider;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.providers.CounterStateMachineProvider;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.providers.StateMachineProvider;
import lombok.val;
import org.apache.ratis.protocol.RaftGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;

@Component
public class ClusterManagementMultiRaftServer extends MultiRaftReplicationServer {

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
        return List.of(
                ClusterMetaDataStateMachineProvider.of("metadata", peers),
                ClusterManagementStateMachineProvider.of("keeper", peers),
                CounterStateMachineProvider.of("counter", peers));
    }

    @Autowired
    public ClusterManagementMultiRaftServer(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super(raftConfig, metaDataClient);
    }
}
