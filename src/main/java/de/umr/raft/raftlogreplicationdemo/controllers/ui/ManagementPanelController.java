package de.umr.raft.raftlogreplicationdemo.controllers.ui;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ManagementPanelController {

    @RequestMapping(value = {"/admin", "/admin/*",
            "/admin/sys-info", "/admin/sys-info/*",
            "/admin/raft-groups", "/admin/raft-groups/*",
            "/admin/meta-data", "/admin/meta-data/*",
            "/admin/replicated-event-store/", "/admin/replicated-event-store/streams/*",
            "/admin/embedded-event-store/", "/admin/embedded-event-store/streams/*",
            "/admin/sys-info/nodes/*"})
    public String index() {
        return "index";
    }
}
