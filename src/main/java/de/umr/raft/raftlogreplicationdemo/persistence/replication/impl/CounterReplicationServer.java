package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.CounterStateMachine;
import lombok.val;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

public class CounterReplicationServer extends RaftReplicationServer<CounterStateMachine> {

    public final static String SERVER_NAME = "counter-replication";

    @Override
    protected String getServerName() {
        return SERVER_NAME;
    }

//    @Override
//    protected UUID getDefaultRaftGroupUUID() {
//        return UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");
//    }

    // TODO raft config changes MUST be told in advance as last config is persisted in ratis own metadata which confuses it on startup otherwhise!
    // 2021-08-30 22:34:27.583  INFO 23272 --- [pool-2-thread-1] o.a.ratis.server.RaftServer$Division     : n1@group-ABB3109A44C1: ConfigurationManager, init=-1: [n1|rpc:localhost:6050|priority:0], old=null, confs=<EMPTY_MAP>

    @Override
    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        String host = raftConfig.getHostAddress();
        val peer = RaftPeer.newBuilder().setId(raftConfig.getCurrentPeerId()).setAddress(host + ":" + raftConfig.getReplicationPort()).build();

        RaftGroupId raftGroupId = RaftGroupId.valueOf(raftGroupUUID);
        return RaftGroup.valueOf(raftGroupId, peer);
    }

    // TODO use metadata to add other peers

    public CounterReplicationServer(RaftConfig raftConfig, ClusterMetadataReplicationClient metaDataClient) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super(raftConfig, metaDataClient, CounterStateMachine.class);
    }
}
