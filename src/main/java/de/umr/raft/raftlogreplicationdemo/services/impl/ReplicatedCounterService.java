package de.umr.raft.raftlogreplicationdemo.services.impl;

import de.umr.raft.raftlogreplicationdemo.models.counter.CreateCounterRequest;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.CounterOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.counter.CounterOperationMessage;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.CounterReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.EventStoreReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.CounterStateMachine;
import de.umr.raft.raftlogreplicationdemo.services.ICounterService;
import de.umr.raft.raftlogreplicationdemo.services.ReplicatedService;
import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.SystemInfoService;
import de.umr.raft.raftlogreplicationdemo.util.RaftGroupUtil;
import lombok.val;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class ReplicatedCounterService extends ReplicatedService implements ICounterService {

    @Autowired
    ClusterManager clusterManager;

    @Autowired
    SystemInfoService systemInfoService;

    Map<String, PartitionInfo> partitions = new HashMap<>();

    /**
     * Obtains a client from the registry or creates a new one if missing
     */
    private CounterReplicationClient createClientForCounterId(String counterId) {
        val partition = partitions.get(counterId);
        var partitionName = counterId; //partition.getPartitionName().getName();
        return CounterReplicationClient.of(
                raftConfig,
                partitionName,
                partition.getRaftGroup());
    }

    private CompletableFuture<Integer> sendAndExecuteOperationMessage(String counterId, CounterOperationMessage operationMessage) {
        return wrapInCompletableFuture(() -> {
            val client = createClientForCounterId(counterId);

            val result = client.sendAndExecuteOperationMessage(
                    operationMessage,
                    CounterOperationResultProto.parser());

            if (!result.getStatus().equals(OperationResultStatus.OK)) {
                // TODO better, custom expection; like StateMachineMessageExecutionException
                throw new UnsupportedOperationException();
            }

            return result.getCounterValue();
        }, counterId);
    }

    @Override
    public CompletableFuture<List<String>> getCounters() throws IOException, ExecutionException, InterruptedException {
        return wrapInCompletableFuture(() -> {
            val counterPartitions = clusterManager.listPartitions(CounterStateMachine.class);

            counterPartitions.forEach(partitionInfo -> partitions.put(partitionInfo.getPartitionName().getName(), partitionInfo));

            return counterPartitions.stream().map(partitionInfo -> partitionInfo.getPartitionName().getName()).collect(Collectors.toList());

            /*val raftGroups = systemInfoService.getRaftGroups();
            val eventStoreRaftGroups = RaftGroupUtil.filterRaftGroupsByStateMachine(raftGroups, CounterStateMachine.class);
            val counterIds = RaftGroupUtil.getPartitionNamesFromRaftGroupInfos(eventStoreRaftGroups);

            return counterIds;*/
        });
    }

    /**
     * Creates a new counter and returns its RaftGroupInfo.
     * If id already given, it throws an exception
     * @return Info about the created raft group
     */
    @Override
    public CompletableFuture<PartitionInfo> createNewCounter(CreateCounterRequest createCounterRequest) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ExecutionException, InterruptedException {
        return wrapInCompletableFuture(() -> {
            val partition = clusterManager.registerPartition(
                    CounterStateMachine.class,
                    createCounterRequest.getId(),
                    createCounterRequest.getPartitionsCount());
            partitions.put(createCounterRequest.getId(), partition);
            return partition;
            /*// TODO should allow passing list of peers for this counter partition
            val peers = raftConfig.getManagementPeersList();
            // TODO this is something that must be done by clusterManagementStateMachine via client, not directly by server
            val raftGroupInfo = clusterManagementMultiRaftServer.registerNewRaftGroup(
                    CounterStateMachineProvider.of(createCounterRequest.getId(), peers));
            return raftGroupInfo;*/
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
