package de.umr.raft.raftlogreplicationdemo.controllers.sysinfo;

import de.umr.raft.raftlogreplicationdemo.models.sysinfo.NodeInfo;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.SystemInfo;
import de.umr.raft.raftlogreplicationdemo.services.sysinfo.SystemInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
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

    @GetMapping("nodes/{id}")
    public NodeInfo getNodeInfo(@PathVariable String id) throws IOException, ExecutionException, InterruptedException {
        return systemInfoService.getNodeInfo(id);
    }

}
