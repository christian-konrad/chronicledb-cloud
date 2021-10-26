package de.umr.raft.raftlogreplicationdemo.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.MultiRaftReplicationServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.lang.reflect.InvocationTargetException;

abstract class MultiRaftReplicationServerStartupRunner<ReplicationServer extends MultiRaftReplicationServer> implements ApplicationRunner {

    protected static final Logger LOG =
            LoggerFactory.getLogger(MultiRaftReplicationServerStartupRunner.class);

    protected ReplicationServer raftReplicationServer;
    private final RaftConfig raftConfig;
    private final ClusterMetadataReplicationClient metaDataClient;

    @Autowired
    public MultiRaftReplicationServerStartupRunner(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient, MultiRaftReplicationServer multiRaftReplicationServer) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.raftConfig = raftConfig;
        this.metaDataClient = metaDataClient;
        this.raftReplicationServer = (ReplicationServer) multiRaftReplicationServer;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOG.info("Application started with option names : {}",
                args.getOptionNames());

        // TODO catch exceptions to prevent it from toggling a "Error starting ApplicationContext"
        // TODO what to do with those exceptions?
        try {
            raftReplicationServer.start();
        } catch (Exception e) {
            e.printStackTrace();
            // noop
        }
    }

}
