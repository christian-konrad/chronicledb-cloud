package de.umr.raft.raftlogreplicationdemo.services.impl.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.DetachPartitionRequest;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.PartitionInfoResponse;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RaftPeerRequest;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RegisterPartitionRequest;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.ClusterHealth;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ClusterManagementService {

    private final ClusterManager clusterManager;

    @Autowired
    public ClusterManagementService(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public PartitionInfoResponse registerPartition(RegisterPartitionRequest registerPartitionRequest) throws ClassNotFoundException {
        var partitionInfo = clusterManager.registerPartition(
                registerPartitionRequest.getStateMachineClass(),
                registerPartitionRequest.getPartitionName(),
                registerPartitionRequest.getReplicationFactor());
        return PartitionInfoResponse.of(partitionInfo);
    }

    public PartitionInfoResponse detachPartition(DetachPartitionRequest registerPartitionRequest) throws ClassNotFoundException {
        var partitionInfo = clusterManager.detachPartition(
                registerPartitionRequest.getStateMachineClass(),
                registerPartitionRequest.getPartitionName());
        return PartitionInfoResponse.of(partitionInfo);
    }

    public List<PartitionInfoResponse> listPartitions() {
        var partitionInfos = clusterManager.listPartitions();
        return partitionInfos.stream().map(PartitionInfoResponse::of).collect(Collectors.toList());
    }

    public List<PartitionInfoResponse> listPartitions(String stateMachineClassname) throws ClassNotFoundException {
        var stateMachineClass = Class.forName(stateMachineClassname).asSubclass(BaseStateMachine.class);
        var partitionInfos = clusterManager.listPartitions(stateMachineClass);
        return partitionInfos.stream().map(PartitionInfoResponse::of).collect(Collectors.toList());
    }

    public void sendHeartbeat(RaftPeerRequest raftPeerRequest) {
        clusterManager.sendHeartbeat(raftPeerRequest.getRaftPeer());
    }

    public ClusterHealth getClusterHealth() {
        return clusterManager.getClusterHealth();
    }
}
