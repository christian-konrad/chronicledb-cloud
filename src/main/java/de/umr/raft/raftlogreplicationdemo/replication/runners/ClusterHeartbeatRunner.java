package de.umr.raft.raftlogreplicationdemo.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementMultiRaftServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.System.currentTimeMillis;

//@Component
@Deprecated
@RequiredArgsConstructor(staticName = "of")
public class ClusterHeartbeatRunner implements ApplicationRunner {

    protected static final Logger LOG =
            LoggerFactory.getLogger(ClusterHeartbeatRunner.class);

    @Autowired
    private final ClusterMetadataReplicationClient client;

    @Autowired
    private final RaftConfig raftConfig;

    @Autowired
    private final ClusterManagementMultiRaftServer clusterManagementMultiRaftServer;

    private Daemon daemon = null;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        daemon = new Daemon(new HeartbeatSender(),
                "heartbeat-sender-" + raftConfig.getCurrentPeerId());
        daemon.start();
    }

    private class HeartbeatSender implements Runnable {

        ReplicatedMetadataMap peerMetaData;
        String currentPeerId;

        HeartbeatSender() {
            currentPeerId = raftConfig.getCurrentPeerId();
            peerMetaData = ReplicatedMetadataMap.of(currentPeerId, client);
        }

        @Override
        public void run() {

            // TODO have custom heartbeat service
            // TODO have heartbeats in the management state machine AND DO NOT PERSIST AND REPLAY THE WHOLE LOG (need snapshots!)

            while (true) {
                try {
                    try {
                        // also obtain and put current division infos
                        if (clusterManagementMultiRaftServer.isRunning()) {
                            peerMetaData.putAsync("heartbeat", String.valueOf(currentTimeMillis()));
                            try {
                                // division is the role this node has in the given group
                                List<RaftServer.Division> divisions = clusterManagementMultiRaftServer.getAllDivisions();
                                for (RaftServer.Division division : divisions) {
                                    val divisionMetaData = ReplicatedMetadataMap.ofDivision(division.getMemberId().toString(), client);
                                    val currentTerm = division.getInfo().getCurrentTerm();
                                    val lastAppliedIndex = division.getInfo().getLastAppliedIndex();
                                    val role = division.getInfo().getCurrentRole();
                                    val isAlive = division.getInfo().isAlive();

                                    // val serverMetrics = division.getRaftServerMetrics();
                                    // val logMetrics = division.getRaftLog().getRaftLogMetrics().toString();

                                    divisionMetaData.putAsync("currentTerm", String.valueOf(currentTerm));
                                    divisionMetaData.putAsync("lastAppliedIndex", String.valueOf(lastAppliedIndex));
                                    divisionMetaData.putAsync("role", role.name());
                                    divisionMetaData.putAsync("isAlive", String.valueOf(isAlive));

                                    // divisionMetaData.putAsync("serverMetrics", serverMetrics.toString());
                                    // divisionMetaData.putAsync("logMetrics", String.valueOf(logMetrics));
                                }
                            } catch (IOException | InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (ExecutionException e) {
                        LOG.warn("Heartbeat request failed with exception", e);
                    } catch (InvalidProtocolBufferException e) {
                        LOG.warn("Heartbeat request failed with exception", e);
                    }

                    Thread.sleep(raftConfig.getHeartbeatInterval());

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

}


