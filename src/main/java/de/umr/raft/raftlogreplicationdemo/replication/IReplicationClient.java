package de.umr.raft.raftlogreplicationdemo.replication;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface IReplicationClient {

    interface Raft {
        abstract CompletableFuture<RaftClientReply> send(Message message) throws IOException;

        abstract CompletableFuture<RaftClientReply> sendReadOnly(Message message) throws IOException;
    }
}
