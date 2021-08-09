package de.umr.raft.raftlogreplicationdemo.persistence.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.IReplicationServer;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.RaftReplicationServer;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

abstract class RaftReplicationServerStartupRunner<ReplicationServer extends RaftReplicationServer> implements ApplicationRunner {

    protected static final Logger LOG =
            LoggerFactory.getLogger(RaftReplicationServerStartupRunner.class);

    protected ReplicationServer raftReplicationServer;
    private final RaftConfig raftConfig;
    private final Constructor<? extends ReplicationServer> serverConstructor;

    @Autowired
    public RaftReplicationServerStartupRunner(RaftConfig raftConfig, Class<? extends ReplicationServer> serverConstructorImpl) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.raftConfig = raftConfig;
        this.serverConstructor = serverConstructorImpl.getConstructor(RaftConfig.class);
        // this.serverConstructor = serverConstructorImpl.getConstructor();
        this.raftReplicationServer = this.serverConstructor.newInstance(raftConfig);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOG.info("Application started with option names : {}",
                args.getOptionNames());

        // TODO server starts twice!
        raftReplicationServer.start();
    }

}
