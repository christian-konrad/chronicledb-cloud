package de.umr.raft.raftlogreplicationdemo.services;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.CounterReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.RaftReplicationClient;
import de.umr.raft.raftlogreplicationdemo.util.FutureUtil;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public abstract class ReplicatedService<ReplicationClientImpl extends RaftReplicationClient> {

    @Autowired
    protected RaftConfig raftConfig;

    // TODO try to replace with CompletableFuture.supplyAsync(callable);
    protected <ReturnType> CompletableFuture<ReturnType> wrapInCompletableFuture(Callable<ReturnType> callable) {
        return FutureUtil.wrapInCompletableFuture(callable);
    }

    protected <ReturnType> CompletableFuture<ReturnType> wrapInCompletableFuture(Callable<ReturnType> callable, String threadPoolKey) {
        return FutureUtil.wrapInCompletableFuture(callable, threadPoolKey);
    }
}
