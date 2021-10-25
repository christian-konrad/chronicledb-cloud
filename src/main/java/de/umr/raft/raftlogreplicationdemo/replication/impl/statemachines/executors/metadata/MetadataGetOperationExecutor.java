package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataQueryOperationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class MetadataGetOperationExecutor implements MetadataQueryOperationExecutor {
@Getter private final MetadataOperationProto metaDataOperation;

    @Override
    public CompletableFuture<MetadataOperationResultProto> apply(Map<String, Map<String, String>> metadata) {
        val scopeId = metaDataOperation.getScopeId();
        val key = metaDataOperation.getKey();

        if (!metadata.containsKey(scopeId)) {
            // TODO should return error message. May use grpc defaults?
            return CompletableFuture.completedFuture(createUnsuccessfulOperationResult());
        }

        String value = metadata.get(scopeId).get(key);

        return CompletableFuture.completedFuture(createMetadataOperationResult(value));
    }

    private MetadataOperationResultProto createUnsuccessfulOperationResult() {
        return MetadataOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResult(MetadataQueryOperationExecutor.createResultMapLeaf(null))
                .setStatus(OperationResultStatus.ERROR)
                .build();
    }

    private MetadataOperationResultProto createMetadataOperationResult(String value) {
        return MetadataOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResult(MetadataQueryOperationExecutor.createResultMapLeaf(value))
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public MetadataOperationType getOperationType() {
        return MetadataOperationType.GET;
    }
}
