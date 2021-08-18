package de.umr.raft.raftlogreplicationdemo.controllers.ui;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class AdminController {

    @RequestMapping(value = { "/admin/*", "/admin"})
    public String index() {
        return "index";
    }
}
