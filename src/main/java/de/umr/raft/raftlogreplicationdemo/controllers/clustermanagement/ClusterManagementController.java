package de.umr.raft.raftlogreplicationdemo.controllers.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.DetachPartitionRequest;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.PartitionInfoResponse;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RaftPeerRequest;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RegisterPartitionRequest;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.ClusterHealth;
import de.umr.raft.raftlogreplicationdemo.services.impl.clustermanagement.ClusterManagementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/cluster-manager/")
public class ClusterManagementController {

    @Autowired
    ClusterManagementService clusterManagementService;

    @PostMapping(value = "/partitions")
    public PartitionInfoResponse registerPartition(@RequestBody RegisterPartitionRequest registerPartitionRequest) throws ClassNotFoundException {
        return clusterManagementService.registerPartition(registerPartitionRequest);
    }

    @DeleteMapping(value = "/partitions/{stateMachineClassname}/{partitionName}")
    public PartitionInfoResponse detachPartition(@PathVariable String stateMachineClassname, @PathVariable String partitionName) throws ClassNotFoundException {
        return clusterManagementService.detachPartition(new DetachPartitionRequest(stateMachineClassname, partitionName));
    }

    @GetMapping(value = "/partitions")
    public List<PartitionInfoResponse> listPartitions() {
        return clusterManagementService.listPartitions();
    }

    @GetMapping(value = "/partitions/{stateMachineClassname}")
    public List<PartitionInfoResponse> listPartitions(@PathVariable String stateMachineClassname) throws ClassNotFoundException {
        return clusterManagementService.listPartitions(stateMachineClassname);
    }

    // TODO remove after test
    @PostMapping(value = "/heartbeat")
    public void sendHeartbeat(@RequestBody RaftPeerRequest raftPeerRequest) {
        clusterManagementService.sendHeartbeat(raftPeerRequest);
    }

    @GetMapping(value = "/health")
    public ClusterHealth getClusterHealth() {
        return clusterManagementService.getClusterHealth();
    }
}
