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

    @GetMapping("raft-groups/{id}/divisions")
    public Map<String, DivisionInfo> getGroupDivisions(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getRaftGroupDivisions(id);
    }

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
