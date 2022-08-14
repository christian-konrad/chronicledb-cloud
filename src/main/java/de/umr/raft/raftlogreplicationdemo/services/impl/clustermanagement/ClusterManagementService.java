package de.umr.raft.raftlogreplicationdemo.services.impl.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RaftPeerRequest;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RegisterPartitionRequest;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClusterManagementService {

    private final ClusterManager clusterManager;

    @Autowired
    public ClusterManagementService(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public PartitionInfo registerPartition(RegisterPartitionRequest registerPartitionRequest) throws ClassNotFoundException {
        return clusterManager.registerPartition(
                registerPartitionRequest.getStateMachineClass(),
                registerPartitionRequest.getPartitionName(),
                registerPartitionRequest.getReplicationFactor());
    }

    public void sendHeartbeat(RaftPeerRequest raftPeerRequest) {
        clusterManager.sendHeartbeat(raftPeerRequest.getRaftPeer());
    }
}
