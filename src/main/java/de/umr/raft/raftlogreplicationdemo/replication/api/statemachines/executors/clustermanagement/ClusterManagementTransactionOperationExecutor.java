package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.clustermanagement;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.ClusterManagementOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.TransactionOperationExecutor;

public interface ClusterManagementTransactionOperationExecutor extends ClusterManagementOperationExecutor<ClusterManagementOperationResultProto>, TransactionOperationExecutor {
}