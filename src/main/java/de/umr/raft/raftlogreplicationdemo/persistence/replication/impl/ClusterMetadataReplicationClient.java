package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata.MetadataOperation;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata.MetadataOperationMessage;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class ClusterMetadataReplicationClient extends RaftReplicationClient {

    public static final Message GET_MESSAGE = Message.valueOf("GET");

    public static MetadataOperationMessage createSetOperationMessage(String nodeId, String key, String value) {
        return new MetadataOperationMessage(MetadataOperation.Utils.createMetaDataSetOperation(nodeId, key, value));
    }

    public static MetadataOperationMessage createDeleteOperationMessage(String nodeId, String key) {
        return new MetadataOperationMessage(MetadataOperation.Utils.createMetaDataDeleteOperation(nodeId, key));
    }

    @Override
    protected UUID getRaftGroupUUID() {
//        return UUID.nameUUIDFromBytes("cluster-metadata".getBytes(StandardCharsets.UTF_8));
        return UUID.nameUUIDFromBytes((String.format("%s:metadata", ClusterManagementMultiRaftServer.SERVER_NAME)).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        return raftConfig.getManagementRaftGroup(getRaftGroupUUID());
    }

    @Autowired
    public ClusterMetadataReplicationClient(RaftConfig raftConfig) {
        super(raftConfig);
    }
}
