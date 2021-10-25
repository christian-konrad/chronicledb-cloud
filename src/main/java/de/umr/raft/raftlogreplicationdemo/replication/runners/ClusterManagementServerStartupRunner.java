package de.umr.raft.raftlogreplicationdemo.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementServer;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.InvocationTargetException;


//@Component
@Deprecated
public class ClusterManagementServerStartupRunner extends RaftReplicationServerStartupRunner<ClusterManagementServer> {

    @Autowired
    public ClusterManagementServerStartupRunner(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        super(raftConfig, metaDataClient, ClusterManagementServer.class);
    }
}
