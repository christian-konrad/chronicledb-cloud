package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataQueryOperationExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class MetadataGetAllOperationExecutor implements MetadataQueryOperationExecutor {
@Getter private final MetadataOperationProto metaDataOperation;

    @Override
    public CompletableFuture<MetadataOperationResultProto> apply(Map<String, Map<String, String>> metadata) {
        return CompletableFuture.completedFuture(createMetadataOperationResult(metadata));
    }

    // slightly complicated way to build a map, but that's what protobuf needs here (as we support multi-level maps)
    private MetadataOperationResultProto createMetadataOperationResult(Map<String, Map<String, String>> metadata) {
        val resultMap = MetadataQueryOperationExecutor.createResultMap(metadata);

        return MetadataOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setResult(resultMap)
                .setStatus(OperationResultStatus.OK)
                .build();
    }

    @Override
    public MetadataOperationType getOperationType() {
        return MetadataOperationType.GET_ALL;
    }
}
