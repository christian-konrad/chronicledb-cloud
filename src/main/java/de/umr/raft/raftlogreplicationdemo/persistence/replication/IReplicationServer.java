package de.umr.raft.raftlogreplicationdemo.persistence.replication;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface IReplicationServer {

    interface Raft {

        public void start() throws IOException;
        // TODO expose state machine methods?
    }
}
