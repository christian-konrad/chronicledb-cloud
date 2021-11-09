package de.umr.raft.raftlogreplicationdemo.services.impl;

import de.umr.raft.raftlogreplicationdemo.models.counter.CreateCounterRequest;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.ClusterManagementMultiRaftServer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.CounterReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.CounterStateMachine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.providers.CounterStateMachineProvider;
import de.umr.raft.raftlogreplicationdemo.services.ICounterService;
import de.umr.raft.raftlogreplicationdemo.services.ReplicatedService;
import de.umr.raft.raftlogreplicationdemo.services.sysinfo.SystemInfoService;
import de.umr.raft.raftlogreplicationdemo.util.RaftGroupUtil;
import lombok.val;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class ReplicatedCounterService extends ReplicatedService implements ICounterService {

    @Autowired
    ClusterManagementMultiRaftServer clusterManagementMultiRaftServer;

    @Autowired
    SystemInfoService systemInfoService;

    private CounterReplicationClient createClientForCounterId(String counterId) {
        return new CounterReplicationClient(raftConfig, counterId);
    }

    private CompletableFuture<Integer> sendAndExecuteOperationMessage(String counterId, CounterOperationMessage operationMessage) {
        return wrapInCompletableFuture(() -> {
            val result = createClientForCounterId(counterId).sendAndExecuteOperationMessage(
                    operationMessage,
                    CounterOperationResultProto.parser());

            if (!result.getStatus().equals(OperationResultStatus.OK)) {
                // TODO better, custom expection; like StateMachineMessageExecutionException
                throw new UnsupportedOperationException();
            }

            return result.getCounterValue();
        });
    }

    @Override
    public CompletableFuture<List<String>> getCounters() throws IOException, ExecutionException, InterruptedException {
        // TODO this is something that must be done by clusterManagementStateMachine via client
        return wrapInCompletableFuture(() -> {
            val raftGroups = systemInfoService.getRaftGroups();
            val eventStoreRaftGroups = RaftGroupUtil.filterRaftGroupsByStateMachine(raftGroups, CounterStateMachine.class);
            val counterIds = RaftGroupUtil.getPartitionNamesFromRaftGroupInfos(eventStoreRaftGroups);
            return counterIds;
        });
    }

    /**
     * Creates a new counter and returns its RaftGroupInfo.
     * If id already given, it throws an exception
     * @return Info about the created raft group
     */
    @Override
    public CompletableFuture<RaftGroupInfo> createNewCounter(CreateCounterRequest createCounterRequest) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ExecutionException, InterruptedException {
        return wrapInCompletableFuture(() -> {
            // TODO should allow passing list of peers for this counter partition
            val peers = raftConfig.getManagementPeersList();
            // TODO this is something that must be done by clusterManagementStateMachine via client, not directly by server
            val raftGroupInfo = clusterManagementMultiRaftServer.registerNewRaftGroup(
                    CounterStateMachineProvider.of(createCounterRequest.getId(), peers));
            return raftGroupInfo;
        });
    }

    @Override
    public CompletableFuture increment(String counterId) throws InvalidProtocolBufferException, ExecutionException, InterruptedException {
        return sendAndExecuteOperationMessage(counterId, CounterReplicationClient.createIncrementOperationMessage());
    }

    @Override
    public CompletableFuture<Integer> getCounter(String counterId) {
        return sendAndExecuteOperationMessage(counterId, CounterReplicationClient.createGetOperationMessage());
    }
}
