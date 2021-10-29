package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultMapProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.QueryOperationExecutor;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

public interface EventStoreQueryOperationExecutor extends EventStoreOperationExecutor<EventStoreOperationResultProto>, QueryOperationExecutor {
//    static MetadataOperationResultMapProto createNullLeaf() {
//        return MetadataOperationResultMapProto.newBuilder()
//                .setIsLeaf(true)
//                .setIsNull(true)
//                .build();
//    }
}
