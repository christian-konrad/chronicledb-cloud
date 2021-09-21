package de.umr.raft.raftlogreplicationdemo.services.impl;

import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.counter.CreateCounterRequest;
import de.umr.raft.raftlogreplicationdemo.models.sysinfo.RaftGroupInfo;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.ClusterManagementMultiRaftServer;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.CounterReplicationClient;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.facades.ReplicatedMetadataMap;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.CounterStateMachine;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.providers.CounterStateMachineProvider;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.sysinfo.RaftSystemInfoClient;
import de.umr.raft.raftlogreplicationdemo.services.ICounterService;
import de.umr.raft.raftlogreplicationdemo.services.sysinfo.SystemInfoService;
import lombok.val;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.css.Counter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.MulticastChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ReplicatedCounterService implements ICounterService {

    @Autowired
    RaftConfig raftConfig;

    @Autowired
    ClusterManagementMultiRaftServer clusterManagementMultiRaftServer;

    @Autowired
    SystemInfoService systemInfoService;

    private final Message INCREMENT_MESSAGE = Message.valueOf("INCREMENT");
    private final Message GET_MESSAGE = Message.valueOf("GET");

    private CounterReplicationClient createClientForCounterId(String counterId) {
        return new CounterReplicationClient(raftConfig, counterId);
    }

    @Override
    public CompletableFuture<List<String>> getCounters() throws IOException, ExecutionException, InterruptedException {
        CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            try {

                val raftGroups = systemInfoService.getRaftGroups();
                val counterRaftGroups = raftGroups.stream().filter(raftGroupInfo -> {
                    val stateMachineClass = raftGroupInfo.getStateMachineClass();
                    val counterStateMachineClassName = CounterStateMachine.class.getCanonicalName();
                    return stateMachineClass.equals(counterStateMachineClassName);
                }).collect(Collectors.toList());
                val counterIds = counterRaftGroups.stream().map(raftGroupInfo ->
                    raftGroupInfo.getName().replace(String.format("%s:", raftGroupInfo.getServerName()), "")
                ).collect(Collectors.toList());

                completableFuture.complete(counterIds);
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
            return null;
        });

        return completableFuture;
    }

    @Override
    public CompletableFuture increment(String counterId) {
        return createClientForCounterId(counterId).send(INCREMENT_MESSAGE);
    }

    @Override
    public CompletableFuture<Integer> getCounter(String counterId) {
        CompletableFuture<Integer> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            try {

                RaftClientReply countReply = createClientForCounterId(counterId).sendReadOnly(GET_MESSAGE).get();
                // TODO catch exceptions or raft errors
                Message countReplyMessage = countReply.getMessage();
                String countMessageContent = countReplyMessage.getContent().toString(Charset.defaultCharset());
                Integer count = countMessageContent.isEmpty() ? 0 : Integer.parseInt(countMessageContent);
                completableFuture.complete(count);
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
            return null;
        });

        return completableFuture;
    }

    /**
     * Creates a new counter and returns its RaftGroupInfo.
     * If id already given, it throws an exception
     * @return Info about the created raft group
     */
    @Override
    public CompletableFuture<RaftGroupInfo> createNewCounter(CreateCounterRequest createCounterRequest) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ExecutionException, InterruptedException {
        CompletableFuture<RaftGroupInfo> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            try {
                val peers = raftConfig.getManagementPeersList();
                val raftGroupInfo = clusterManagementMultiRaftServer.registerNewRaftGroup(
                        CounterStateMachineProvider.of(createCounterRequest.getId(), peers));
                completableFuture.complete(raftGroupInfo);
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
            return null;
        });

        return completableFuture;
    }
}
