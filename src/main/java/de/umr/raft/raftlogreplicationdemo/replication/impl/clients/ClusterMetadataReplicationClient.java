package de.umr.raft.raftlogreplicationdemo.replication.impl.clients;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.metadata.MetadataOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementMultiRaftServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementServer;
import org.apache.ratis.protocol.RaftGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class ClusterMetadataReplicationClient extends RaftReplicationClient<MetadataOperationMessage> {

    public static MetadataOperationMessage createSetOperationMessage(String scopeId, String key, String value) {
        return MetadataOperationMessage.Factory.createSetOperationMessage(scopeId, key, value);
    }

    public static MetadataOperationMessage createDeleteOperationMessage(String scopeId, String key) {
        return MetadataOperationMessage.Factory.createDeleteOperationMessage(scopeId, key);
    }

    public static MetadataOperationMessage createGetOperationMessage(String scopeId, String key) {
        return MetadataOperationMessage.Factory.createGetOperationMessage(scopeId, key);
    }

    public static MetadataOperationMessage createGetAllForScopeOperationMessage(String scopeId) {
        return MetadataOperationMessage.Factory.createGetAllForScopeOperationMessage(scopeId);
    }

    public static MetadataOperationMessage createGetAllOperationMessage() {
        return MetadataOperationMessage.Factory.createGetAllOperationMessage();
    }

    @Override
    protected UUID getRaftGroupUUID() {
        //return UUID.nameUUIDFromBytes((String.format("%s:metadata", ClusterManagementMultiRaftServer.SERVER_NAME)).getBytes(StandardCharsets.UTF_8));
        return ClusterManagementServer.META_GROUP_UUID;
    }

    @Override
    protected RaftGroup getRaftGroup(UUID raftGroupUUID) {
        return raftConfig.getManagementRaftGroup(getRaftGroupUUID());
        //return clusterManagementServer.getMetadataRaftGroup();
    }

    @Autowired
    public ClusterMetadataReplicationClient(RaftConfig raftConfig) {
        super(raftConfig);
    }
}
