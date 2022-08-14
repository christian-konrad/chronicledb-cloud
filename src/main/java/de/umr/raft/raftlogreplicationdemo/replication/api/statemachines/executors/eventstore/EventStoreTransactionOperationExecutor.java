package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.TransactionOperationExecutor;

public interface EventStoreTransactionOperationExecutor extends EventStoreOperationExecutor<EventStoreOperationResultProto>, TransactionOperationExecutor {}
