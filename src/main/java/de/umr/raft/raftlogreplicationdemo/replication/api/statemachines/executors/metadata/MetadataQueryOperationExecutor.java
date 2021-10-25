package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultMapProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.QueryOperationExecutor;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

public interface MetadataQueryOperationExecutor extends MetadataOperationExecutor<MetadataOperationResultProto>, QueryOperationExecutor {
    static MetadataOperationResultMapProto createNullLeaf() {
        return MetadataOperationResultMapProto.newBuilder()
                .setIsLeaf(true)
                .setIsNull(true)
                .build();
    }

    static MetadataOperationResultMapProto createResultMapLeaf(String value) {
        if (value == null) return createNullLeaf();
        return MetadataOperationResultMapProto.newBuilder()
                .setIsLeaf(true)
                .setLeafValue(value)
                .build();
    }

    static MetadataOperationResultMapProto createResultMapNode(Map<String, String> map) {
        if (map == null) return createNullLeaf();

        return MetadataOperationResultMapProto.newBuilder()
                .setIsLeaf(false)
                .putAllNodes(map.entrySet().stream()
                        .map(entry -> new AbstractMap.SimpleEntry<>(
                                entry.getKey(),
                                MetadataQueryOperationExecutor.createResultMapLeaf(entry.getValue())))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )))
                .build();
    }

    static MetadataOperationResultMapProto createResultMap(Map<String, Map<String, String>> map) {
        if (map == null) return createNullLeaf();

        return MetadataOperationResultMapProto.newBuilder()
                .setIsLeaf(false)
                .putAllNodes(map.entrySet().stream()
                        .map(entry -> new AbstractMap.SimpleEntry<>(
                                entry.getKey(),
                                MetadataQueryOperationExecutor.createResultMapNode(entry.getValue())))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )))
                .build();
    }
}
