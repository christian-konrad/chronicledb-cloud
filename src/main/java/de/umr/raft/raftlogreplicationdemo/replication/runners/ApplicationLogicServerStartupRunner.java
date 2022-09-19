package de.umr.raft.raftlogreplicationdemo.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ApplicationLogicServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementMultiRaftServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.MultiRaftReplicationServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterManagementClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;

//@Component
@Deprecated
public class ApplicationLogicServerStartupRunner implements ApplicationRunner {

    @Autowired
    protected static final Logger LOG =
            LoggerFactory.getLogger(ApplicationLogicServerStartupRunner.class);

    protected ApplicationLogicServer applicationLogicServer;
    protected ClusterManagementServer clusterManagementServer;
    //private final RaftConfig raftConfig;
    //private final ClusterManager clusterManager;

    @Autowired
    public ApplicationLogicServerStartupRunner(ApplicationLogicServer applicationLogicServer, ClusterManagementServer clusterManagementServer) {
        //this.raftConfig = raftConfig;
        //this.clusterManager = clusterManager;
        this.applicationLogicServer = applicationLogicServer;
        this.clusterManagementServer = clusterManagementServer;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOG.info("Raft application server started with options : {}",
                args.getOptionNames());

        try {
            // wait until management cluster quorum runs
            while (!clusterManagementServer.isRunning()) {}

            applicationLogicServer.start();
        } catch (Exception e) {
            e.printStackTrace();
            // noop
        }
    }
}
