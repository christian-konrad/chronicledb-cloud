package de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.performance;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class MeasureMetadataPerformanceService extends MeasurePerformanceService {

    protected static final Logger LOG =
            LoggerFactory.getLogger(MeasureMetadataPerformanceService.class);

    @Autowired
    ClusterMetadataReplicationClient metadataReplicationClient;

    @Autowired
    RaftConfig raftConfig;

    CompletableFuture<Void> testGetMetadata(int count) {
        return FutureUtil.wrapInCompletableFuture(() -> {
            ReplicatedMetadataMap replicatedMetaDataMap = ReplicatedMetadataMap.of(raftConfig.getCurrentPeerId(), metadataReplicationClient);
            for (int i = 0; i < count; i++) {
                replicatedMetaDataMap.get("heartbeat");
            };
            return null;
        });
    }

    public CompletableFuture<String> runGetMetadataMeasurements() {
        return runMeasurement(
                testGetMetadata(1000),
                "Got a metadata entry 1000 times"
        );
    }
}
