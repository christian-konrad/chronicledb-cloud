package de.umr.raft.raftlogreplicationdemo.replication;

import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IReplicationServer {

    interface Raft {

        public void start() throws IOException, ExecutionException, InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
        // TODO expose state machine methods?

    }
}
