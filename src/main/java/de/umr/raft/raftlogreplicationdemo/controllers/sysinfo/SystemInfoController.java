package de.umr.raft.raftlogreplicationdemo.controllers.sysinfo;

import de.umr.raft.raftlogreplicationdemo.models.Greeting;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.NodeInfo;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.SystemInfo;
import de.umr.raft.raftlogreplicationdemo.services.sysinfo.SystemInfoService;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@RestController
public class SystemInfoController {

    @Autowired
    SystemInfoService systemInfoService;

    @GetMapping("/api/sys-info")
    public SystemInfo getSystemInfo() throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getSystemInfo();
    }

    @GetMapping("/api/sys-info/nodes/{id}")
    public NodeInfo getNodeInfo(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getNodeInfo(id);
    }

}
