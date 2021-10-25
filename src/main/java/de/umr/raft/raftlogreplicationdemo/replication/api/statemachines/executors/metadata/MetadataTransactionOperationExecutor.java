package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultMapProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.TransactionOperationExecutor;

public interface MetadataTransactionOperationExecutor extends MetadataOperationExecutor<MetadataOperationResultProto>, TransactionOperationExecutor {}
