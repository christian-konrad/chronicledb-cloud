package de.umr.raft.raftlogreplicationdemo.services.impl;

import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.CounterReplicationClient;
import de.umr.raft.raftlogreplicationdemo.services.ICounterService;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ReplicatedCounterService implements ICounterService {

    @Autowired
    CounterReplicationClient replicationClient;

    private final Message INCREMENT_MESSAGE = Message.valueOf("INCREMENT");
    private final Message GET_MESSAGE = Message.valueOf("GET");

    @Override
    public CompletableFuture increment() {
        return replicationClient.send(INCREMENT_MESSAGE);
    }

    @Override
    public CompletableFuture<Integer> getCounter() {
        CompletableFuture<Integer> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            try {
                RaftClientReply countReply = replicationClient.sendReadOnly(GET_MESSAGE).get();
                // TODO catch exceptions or raft errors
                Message countReplyMessage = countReply.getMessage();
                String countMessageContent = countReplyMessage.getContent().toString(Charset.defaultCharset());
                Integer count = Integer.parseInt(countMessageContent);
                completableFuture.complete(count);
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
            return null;
        });

        return completableFuture;
    }
}
