package de.umr.raft.raftlogreplicationdemo.controllers.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RaftPeerRequest;
import de.umr.raft.raftlogreplicationdemo.models.clustermanagement.RegisterPartitionRequest;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.services.impl.clustermanagement.ClusterManagementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cluster-manager/")
public class ClusterManagementController {

    @Autowired
    ClusterManagementService clusterManagementService;

    @PostMapping(value = "/partitions")
    public PartitionInfo registerPartition(@RequestBody RegisterPartitionRequest registerPartitionRequest) throws ClassNotFoundException {
        return clusterManagementService.registerPartition(registerPartitionRequest);
    }

    // TODO remove after test
    @PostMapping(value = "/heartbeat")
    public void sendHeartbeat(@RequestBody RaftPeerRequest raftPeerRequest) throws ClassNotFoundException {
        clusterManagementService.sendHeartbeat(raftPeerRequest);
    }
}
