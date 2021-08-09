package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.IReplicationServer;
import lombok.val;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.UUID;

public abstract class RaftReplicationServer<StateMachine extends BaseStateMachine> implements IReplicationServer.Raft {

    protected static final Logger LOG =
        LoggerFactory.getLogger(RaftReplicationServer.class);

    private final RaftServer raftServer;
    private final RaftConfig raftConfig;
    private StateMachine stateMachine;

    private final Constructor<? extends StateMachine> stateMachineConstructor;

    protected abstract UUID getRaftGroupUUID();

    public RaftReplicationServer(RaftConfig raftConfig, Class<? extends StateMachine> stateMachineImpl) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        this.raftConfig = raftConfig;
        this.stateMachineConstructor = stateMachineImpl.getConstructor();
        this.raftServer = buildRaftServer();
    }
//
//    // unused default constructor to prevent reflection in RaftReplicationServerStartupRunner from crashing
//    protected RaftReplicationServer() {
//        this.raftConfig = null;
//        this.stateMachineConstructor = null;
//        this.raftServer = null;
//    }

    private RaftServer buildRaftServer() throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        RaftGroup raftGroup = raftConfig.getRaftGroup(getRaftGroupUUID());

        //find current peer object based on application parameter
        RaftPeer currentPeer =
                raftGroup.getPeer(RaftPeerId.valueOf(raftConfig.getCurrentPeerId()));

        LOG.info("Current peer id: {}", raftConfig.getCurrentPeerId());

        val properties = new RaftProperties();
        val raftStorageDir = new File(raftConfig.getStoragePath());
        RaftServerConfigKeys.setStorageDir(properties,
                Collections.singletonList(raftStorageDir));
        val port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        //create the actual state machine
        stateMachine = stateMachineConstructor.newInstance();

        //create and start the Raft server
        return RaftServer.newBuilder()
                .setGroup(raftGroup)
                .setProperties(properties)
                .setServerId(currentPeer.getId())
                .setStateMachine(stateMachine)
                .build();

        // TODO it seems like a single raftServer can be included in many RaftGroups, why?
        // TODO it seems like a Devision is the role of a RaftServer in a RaftGroup
        // TODO use raftServer.setConfiguration() for hot config changes ( new peers added etc. )
    }

    public void start() throws IOException {
        LOG.info("Raft server started");
        raftServer.start();
    }


}
