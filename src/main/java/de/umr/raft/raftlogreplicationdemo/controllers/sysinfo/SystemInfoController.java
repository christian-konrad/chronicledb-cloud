package de.umr.raft.raftlogreplicationdemo.controllers.sysinfo;

import de.umr.raft.raftlogreplicationdemo.models.sysinfo.*;
import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.SystemInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/sys-info")
public class SystemInfoController {

    @Autowired
    SystemInfoService systemInfoService;

    @GetMapping("")
    public SystemInfo getSystemInfo() throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getSystemInfo();
    }

    @GetMapping("metadata")
    public Map<String, Map<String, String>> getAllMetaData() throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getAllMetaData();
    }

    /**
     * Returns division info on a raft group, which represents the current
     * state of the group and its nodes of the time of response to the request.
     * This includes the roles of the nodes (LEADER, FOLLOWER, CANDIDATE)
     * and their last applied term and index.
     * @param id The id of the raft group
     * @return The division info, containing the state of the nodes.
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping("raft-groups/{id}/divisions")
    public Map<String, DivisionInfo> getGroupDivisions(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getRaftGroupDivisions(id);
    }

    /**
     * Returns general info of the raft group for the given id.
     * Does not include current state info (division info) which
     * can be requested via `/divisions`.
     * @param id The id of the raft group
     * @return The raft group info, including the associated nodes and names
     * of the state machine, group instance etc.
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping("raft-groups/{id}")
    public RaftGroupInfo getRaftGroupInfo(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getRaftGroupInfo(id);
    }

    @GetMapping("nodes/{id}")
    public NodeInfo getNodeInfo(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getNodeInfo(id);
    }

    // TODO if the cluster is unhealthy, the whole node stops working and cannot inform the user about it's state.
    @GetMapping("cluster/health")
    public ClusterHealth getClusterHealth() throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getClusterHealth();
    }

}
