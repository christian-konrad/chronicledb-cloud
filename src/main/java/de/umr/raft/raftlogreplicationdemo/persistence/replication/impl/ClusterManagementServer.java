package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.ClusterMetadataStateMachine;
import org.apache.ratis.protocol.RaftGroup;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

@Deprecated
public class ClusterManagementServer extends RaftReplicationServer<ClusterMetadataStateMachine> {

    public final static String SERVER_NAME = "cluster-metadata";

    @Override
    protected String getServerName() {
        return SERVER_NAME;
    }

    // TODO to support second raft group for management, need to call
    // client.getGroupManagementApi(peer.getId()).add(group2);
    // so, the server may call itself via a client to add a second group to itself
    // TODO simplify this by adding an interface with registerRaftGroup method
    // that will register a group + corresponding state machine
    // TODO to do this; raftReplicationServer ALWAYS has a defaultGroup that does nothing than
    // representing the base group for the client to add further groups
    // TODO remove getRaftGroup; allow setting of multiple groups
    // which follow the naming SERVER_NAME + "-" + GROUP_NAME
    // TODO may provide custom Class to hold group and state machine together
    // TODO this also simplifies access via StateMachineRegistry


    @Override
    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        return raftConfig.getManagementRaftGroup(raftGroupUUID);
//        List<RaftPeer> peers = raftConfig.getMetadataPeersList();
//
//        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
//        return RaftGroup.valueOf(raftGroupId, peers);
    }

    /*
    TODO pass to state machine. Use same as for displaying in frontend/API
    long failureDetectionPeriod = getConfig().
        getLong(Constants.LOG_SERVICE_PEER_FAILURE_DETECTION_PERIOD_KEY,
        Constants.DEFAULT_PEER_FAILURE_DETECTION_PERIOD);
     */

    public ClusterManagementServer(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super(raftConfig, metaDataClient, ClusterMetadataStateMachine.class);
    }
}
