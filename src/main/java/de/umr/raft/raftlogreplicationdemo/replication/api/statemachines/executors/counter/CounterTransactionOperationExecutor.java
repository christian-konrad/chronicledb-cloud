package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.counter;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.TransactionOperationExecutor;

public interface CounterTransactionOperationExecutor extends CounterOperationExecutor<CounterOperationResultProto>, TransactionOperationExecutor {}
