package de.umr.raft.raftlogreplicationdemo.controllers.sysinfo;

import de.umr.raft.raftlogreplicationdemo.models.Greeting;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.SystemInfo;
import de.umr.raft.raftlogreplicationdemo.services.sysinfo.SystemInfoService;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class SystemInfoController {

    @Autowired
    SystemInfoService systemInfoService;

    @GetMapping("/api/sys-info")
    public SystemInfo getSystemInfo() throws IOException {
        return systemInfoService.getSystemInfo();
    }
}
