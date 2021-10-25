package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataQueryOperationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "of")
public class MetadataGetAllForScopeOperationExecutor implements MetadataQueryOperationExecutor {
    @Getter private final MetadataOperationProto metaDataOperation;

    Logger LOG = LoggerFactory.getLogger(MetadataGetAllForScopeOperationExecutor.class);

    @Override
    public CompletableFuture<MetadataOperationResultProto> apply(Map<String, Map<String, String>> metadata) {
        val scopeId = metaDataOperation.getScopeId();

        Map<String, String> scopeMetadata = metadata.get(scopeId);

        return CompletableFuture.completedFuture(createMetadataOperationResult(scopeMetadata));
    }

    // slightly complicated way to build a map, but that's what protobuf needs here (as we support multi-level maps)
    private MetadataOperationResultProto createMetadataOperationResult(Map<String, String> metadata) {
        val resultMap = MetadataQueryOperationExecutor.createResultMapNode(metadata);

        return MetadataOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResult(resultMap)
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public MetadataOperationType getOperationType() {
        return MetadataOperationType.GET_ALL_FOR_SCOPE;
    }
}
