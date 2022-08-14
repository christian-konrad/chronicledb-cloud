package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataTransactionOperationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor(staticName = "of")
public class DeleteOperationExecutor implements MetadataTransactionOperationExecutor {
@Getter private final MetadataOperationProto metaDataOperation;

    public CompletableFuture<MetadataOperationResultProto> apply(Map<String, Map<String, String>> metadata) {
        val scopeId = metaDataOperation.getScopeId();
        val key = metaDataOperation.getKey();

        Map<String, String> nodeMetadata = metadata.containsKey(scopeId)
                ? metadata.get(scopeId)
                : new ConcurrentHashMap<>();

        String removedValue = nodeMetadata.remove(key);

        if (nodeMetadata.isEmpty()) {
            metadata.remove(scopeId);
        } else {
            metadata.put(scopeId, nodeMetadata);
        }

        return CompletableFuture.completedFuture(createMetadataOperationResult(removedValue));
    }

    private MetadataOperationResultProto createMetadataOperationResult(String removedValue) {
        return MetadataOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResult(MetadataQueryOperationExecutor.createResultMapLeaf(removedValue))
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public MetadataOperationType getOperationType() {
        return MetadataOperationType.DELETE;
    }
}
