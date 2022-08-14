package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataTransactionOperationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor(staticName = "of")
public class SetOperationExecutor implements MetadataTransactionOperationExecutor {
    @Getter private final MetadataOperationProto metaDataOperation;

    @Override
    public CompletableFuture<MetadataOperationResultProto> apply(Map<String, Map<String, String>> metadata) {

        // TODO what about reducer kind of style instead of executors?
        // TODO explain this pattern in the doc (state, executors (that map proto message to state change))

        val scopeId = metaDataOperation.getScopeId();
        val key = metaDataOperation.getKey();
        val value = metaDataOperation.getValue();

        // TODO catch errors and return success = false

        Map<String, String> nodeMetadata = metadata.containsKey(scopeId)
                ? metadata.get(scopeId)
                : new ConcurrentHashMap<>();

        if (value == null) {
            nodeMetadata.remove(key);
        } else {
            nodeMetadata.put(key, value);
        }

        if (nodeMetadata.isEmpty()) {
            metadata.remove(scopeId);
        } else {
            metadata.put(scopeId, nodeMetadata);
        }

        return CompletableFuture.completedFuture(createMetadataOperationResult(value));
    }

    private MetadataOperationResultProto createMetadataOperationResult(String newValue) {
        return MetadataOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResult(MetadataQueryOperationExecutor.createResultMapLeaf(newValue))
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public MetadataOperationType getOperationType() {
        return MetadataOperationType.SET;
    }
}
