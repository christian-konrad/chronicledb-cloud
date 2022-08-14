package de.umr.raft.raftlogreplicationdemo.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;

@Component
public class ClusterManagementServerStartupRunner implements ApplicationRunner {

    protected static final Logger LOG =
            LoggerFactory.getLogger(ClusterManagementServerStartupRunner.class);

    protected ClusterManagementServer clusterManagementServer;
    private final RaftConfig raftConfig;

    @Autowired
    public ClusterManagementServerStartupRunner(RaftConfig raftConfig, ClusterManagementServer clusterManagementServer) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.raftConfig = raftConfig;
        this.clusterManagementServer = clusterManagementServer;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOG.info("Cluster management server started with option names : {}",
                args.getOptionNames());

        try {
            clusterManagementServer.start();
        } catch (Exception e) {
            e.printStackTrace();
            // noop
        }
    }
}
