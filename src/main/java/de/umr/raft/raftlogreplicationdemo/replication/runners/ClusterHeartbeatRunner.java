package de.umr.raft.raftlogreplicationdemo.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.ReplicatedMetadataMap;
import lombok.RequiredArgsConstructor;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

import static java.lang.System.currentTimeMillis;

//@Component
@RequiredArgsConstructor(staticName = "of")
public class ClusterHeartbeatRunner implements ApplicationRunner {

    protected static final Logger LOG =
            LoggerFactory.getLogger(ClusterHeartbeatRunner.class);

    @Autowired
    private final ClusterMetadataReplicationClient client;

    @Autowired
    private final RaftConfig raftConfig;

    private Daemon daemon = null;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        daemon = new Daemon(new HeartbeatSender(),
                "heartbeat-sender-" + raftConfig.getCurrentPeerId());
        daemon.start();
    }

    private class HeartbeatSender implements Runnable {

        ReplicatedMetadataMap replicatedMetaDataMap;

        HeartbeatSender() {
            replicatedMetaDataMap = ReplicatedMetadataMap.of(raftConfig.getCurrentPeerId(), client);
        }

        @Override
        public void run() {

            while (true) {
                try {
                    replicatedMetaDataMap.put("heartbeat", String.valueOf(currentTimeMillis()));
                    Thread.sleep(raftConfig.getHeartbeatInterval());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ExecutionException e) {
                    LOG.warn("Heartbeat request failed with exception", e);
                } catch (InvalidProtocolBufferException e) {
                    LOG.warn("Heartbeat request failed with exception", e);
                }
            }

        }
    }

}


