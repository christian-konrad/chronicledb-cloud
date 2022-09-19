package de.umr.raft.raftlogreplicationdemo.replication.runners;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ApplicationLogicServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata.ReplicatedMetadataMap;
import lombok.val;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.system.JavaVersion;
import org.springframework.boot.system.SystemProperties;
import org.springframework.core.SpringVersion;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.concurrent.ExecutionException;

@Component
public class RaftServersStartupRunner implements ApplicationRunner {

    @Autowired
    protected static final Logger LOG =
            LoggerFactory.getLogger(RaftServersStartupRunner.class);

    protected ApplicationLogicServer applicationLogicServer;
    protected ClusterManagementServer clusterManagementServer;
    private final RaftConfig raftConfig;
    private final ClusterMetadataReplicationClient clusterMetadataClient;
    //private final ClusterManager clusterManager;

    @Autowired
    public RaftServersStartupRunner(RaftConfig raftConfig, ApplicationLogicServer applicationLogicServer, ClusterManagementServer clusterManagementServer, ClusterMetadataReplicationClient clusterMetadataClient) {
        this.raftConfig = raftConfig;
        //this.clusterManager = clusterManager;
        this.applicationLogicServer = applicationLogicServer;
        this.clusterManagementServer = clusterManagementServer;
        this.clusterMetadataClient = clusterMetadataClient;
    }

    private void broadcastNodeMetadata() throws InvalidProtocolBufferException, ExecutionException, InterruptedException, UnknownHostException {
        val peers = raftConfig.getManagementPeersList();
        val currentPeer = peers.stream().filter(peer -> peer.getId().toString().equals(raftConfig.getCurrentPeerId())).findFirst().orElse(null);

        val currentPeerPort = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();

        val replicatedMetaDataMap = ReplicatedMetadataMap.of(raftConfig.getCurrentPeerId(), clusterMetadataClient);

        replicatedMetaDataMap.put("nodeId", raftConfig.getCurrentPeerId());
        replicatedMetaDataMap.put("storagePath", raftConfig.getStoragePath());
        replicatedMetaDataMap.put("metadataPort", String.valueOf(currentPeerPort));
        replicatedMetaDataMap.put("replicationPort", raftConfig.getReplicationPort());

        replicatedMetaDataMap.put("httpPort", raftConfig.getHttpPort());
        replicatedMetaDataMap.put("localHostAddress", InetAddress.getLocalHost().getHostAddress());
        replicatedMetaDataMap.put("localHostName", InetAddress.getLocalHost().getHostName());

        replicatedMetaDataMap.put("remoteHostAddress", raftConfig.getPublicHostAddress());

        replicatedMetaDataMap.put("osName", System.getProperty("os.name"));
        replicatedMetaDataMap.put("osVersion", System.getProperty("os.version"));

        replicatedMetaDataMap.put("springVersion", SpringVersion.getVersion());
        replicatedMetaDataMap.put("jdkVersion", SystemProperties.get("java.version"));
        replicatedMetaDataMap.put("javaVersion", JavaVersion.getJavaVersion().toString());

        StringBuilder totalDiskSpace = new StringBuilder();
        StringBuilder usableDiskSpace = new StringBuilder();

        NumberFormat nf = NumberFormat.getNumberInstance();
        for (Path root : FileSystems.getDefault().getRootDirectories()) {
            try {
                FileStore store = Files.getFileStore(root);
                totalDiskSpace.append(root).append(" = ")
                        .append(nf.format(store.getTotalSpace()))
                        .append(System.getProperty("line.separator"));
                usableDiskSpace.append(root).append(" = ")
                        .append(nf.format(store.getUsableSpace()))
                        .append(System.getProperty("line.separator"));
            } catch (IOException e) {
                System.out.println("Error querying disk space: " + e.toString());
            }
        }

        replicatedMetaDataMap.put("totalDiskSpace", totalDiskSpace.toString());
        replicatedMetaDataMap.put("usableDiskSpace", usableDiskSpace.toString());

        System.out.println("==============================");
        System.out.println(replicatedMetaDataMap.toString());
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try {
            clusterManagementServer.start();
        } catch (Exception e) {
            e.printStackTrace();
            // noop
        }

        LOG.info("Cluster management server started with options : {}",
                args.getOptionNames());

        broadcastNodeMetadata();

        try {
            applicationLogicServer.start();
        } catch (Exception e) {
            e.printStackTrace();
            // noop
        }

        LOG.info("Raft application server started");
    }
}
