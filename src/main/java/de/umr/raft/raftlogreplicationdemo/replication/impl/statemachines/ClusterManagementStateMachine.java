package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.clustermanagement.ClusterManagementOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterManagementClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.management.ClusterStateManager;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;

public class ClusterManagementStateMachine extends ExecutableMessageStateMachine<ClusterStateManager, ClusterManagementOperationMessage, ClusterManagementOperationResultProto> {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterManagementStateMachine.class);

    private final RaftConfig raftConfig;
    private final ClusterManager clusterManager;

    private final ClusterState clusterState;
    private final ClusterStateManager clusterStateManager;

    public ClusterManagementStateMachine(RaftConfig raftConfig, ClusterManager clusterManager) {
        this.raftConfig = raftConfig;
        this.clusterManager = clusterManager;
        clusterState = ClusterState.createUninitializedState(raftConfig, clusterManager);
        //clusterManagementClient = new ClusterManagementClient(raftConfig);
        clusterStateManager = ClusterStateManager.of(clusterState, getServer());
    }

    // TODO return manager, not state only
    @Override
    protected ClusterStateManager getStateObject() {
        return clusterStateManager;
    }

    @Override
    protected Object createStateSnapshot() {
        return clusterState.createSnapshot();
    }

    @Override
    protected void initState() throws IOException {
        clusterState.initState();
    }

    // TODO ore just a "closeState"?
    @Override
    protected void clearState() {
        clusterState.clear();
    }

    @Override
    protected void restoreState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        clusterState.loadFrom(in);
    }

//    @Override
//    public boolean getShouldLogTransactionRuntime() {
//        return true;
//    }

    @Override
    protected ClusterManagementOperationMessage createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException {
        return ClusterManagementOperationMessage.of(byteString);
    }

    // TODO remove after test
    @Override
    public void beforeApplyTransaction(TransactionContext trx) {
        super.beforeApplyTransaction(trx);
        if (LOG.isDebugEnabled()) {
            final RaftProtos.LogEntryProto entry = trx.getLogEntry();
            LOG.debug("Executing message " + entry.getTerm() + ":" + entry.getIndex());
        }
    }
}
