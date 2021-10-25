package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.counter;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.QueryOperationExecutor;

public interface CounterQueryOperationExecutor extends CounterOperationExecutor<CounterOperationResultProto>, QueryOperationExecutor {}
