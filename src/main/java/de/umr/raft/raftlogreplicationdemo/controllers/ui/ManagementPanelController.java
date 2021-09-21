package de.umr.raft.raftlogreplicationdemo.controllers.ui;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ManagementPanelController {

    @RequestMapping(value = {"/admin", "/admin/*",
            "/admin/sys-info", "/admin/sys-info/*",
            "/admin/raft-groups", "/admin/raft-groups/*",
            "/admin/sys-info/nodes/*"})
    public String index() {
        return "index";
    }
}
