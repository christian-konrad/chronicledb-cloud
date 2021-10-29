package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.metadata.MetadataOperationMessage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterMetadataStateMachine extends ExecutableMessageStateMachine<Map<String, Map<String, String>>, MetadataOperationMessage, MetadataOperationResultProto> {

    // a map of key value pairs per scope (e.g. node id)
    private final Map<String, Map<String, String>> metadata = new ConcurrentHashMap<>();

    // TODO may don't need to take snapshots; may should prevent log replay of heartbeats

    @Override
    protected Map<String, Map<String, String>> getStateObject() {
        return metadata;
    }

    @Override
    protected Object createStateSnapshot() {
        return getStateObject();
    }

    @Override
    protected void initState() {
        // noop
    }

    @Override
    protected void clearState() {
        metadata.clear();
    }

    @Override
    protected void restoreState(ObjectInputStream in) throws IOException, ClassNotFoundException {
        metadata.putAll(JavaUtils.cast(in.readObject()));
    }

    @Override
    protected MetadataOperationMessage createExecutableMessageOf(ByteString byteString) throws InvalidProtocolBufferException {
        return MetadataOperationMessage.of(byteString);
    }
}
