package de.umr.raft.raftlogreplicationdemo.replication.impl.facades;


import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Accessor class for a replicated metadata map (KV pairs) of a certain node.
 * Actually a facade for the ClusterMetadataReplicationClient
 * TODO should prevent writes on other node's content
 */
public class ReplicatedMetadataMap {

    @Getter private final String scope;

    private final ClusterMetadataReplicationClient client;

    private Map<String, String> currentData;

    private ReplicatedMetadataMap(String scope, ClusterMetadataReplicationClient client) {
//        if (scope.getBytes(MetadataOperationMessage.UTF8).length > MetadataSetOperation.NODE_ID_LENGTH_LIMIT) {
//            throw new IllegalArgumentException("Node id too long: Only " + MetadataSetOperation.NODE_ID_LENGTH_LIMIT + " bytes supported");
//        }
        this.scope = scope;
        this.client = client;
    }

    public static ReplicatedMetadataMap of(String scope, ClusterMetadataReplicationClient client) {
        return new ReplicatedMetadataMap(scope, client);
    }

    public static ReplicatedMetadataMap ofRaftGroupScope(ClusterMetadataReplicationClient client) {
        return ReplicatedMetadataMap.of("raft-groups", client);
    }

    private void fetchCurrentData() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val responseProto = client.sendAndExecuteOperationMessage(
                ClusterMetadataReplicationClient.createGetAllForScopeOperationMessage(scope),
                MetadataOperationResultProto.parser());

        if (!responseProto.getStatus().equals(OperationResultStatus.OK)) {
            // TODO throw exception
            currentData = null;
            return;
        }

        val resultMapProto = responseProto.getResult();

        if (resultMapProto.getIsNull()) {
            currentData = new HashMap<>();
            return;
        }

        // simple deserialization as map can only be nested in 2 levels so we only care about leaves
        currentData = resultMapProto.getNodesMap().entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(
                    entry.getKey(),
                    entry.getValue().getLeafValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));

        // val data = (ConcurrentHashMap<String, Map<String, String>>) SerializationUtils.deserialize(response.getContent().toByteArray());
        // currentData = data.get(scope);
    }

    public boolean isEmpty() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        fetchCurrentData();
        // TODO dedicated message operation
        return currentData.isEmpty();
    }

    public boolean containsKey(String key) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        fetchCurrentData();
        // TODO dedicated message operation
        return currentData.containsKey(key);
    }

    public String get(String key) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val responseProto = client.sendAndExecuteOperationMessage(
                ClusterMetadataReplicationClient.createGetOperationMessage(scope, key),
                MetadataOperationResultProto.parser());

        if (!responseProto.getStatus().equals(OperationResultStatus.OK)) {
            throw new NoSuchElementException();
        }

        val result = responseProto.getResult();

        val resultVal = result.getIsNull() ? null : result.getLeafValue();

        return resultVal;

        //return currentData.get(key);
    }

    public String put(String key, String value) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
//        if (key.getBytes(MetadataOperationMessage.UTF8).length > MetadataSetOperation.KEY_LENGTH_LIMIT) {
//            throw new IllegalArgumentException("Key too long: Only " + MetadataSetOperation.KEY_LENGTH_LIMIT + " bytes supported");
//        }
//        if (value.getBytes(MetadataOperationMessage.UTF8).length > MetadataSetOperation.VALUE_LENGTH_LIMIT) {
//            throw new IllegalArgumentException("Value too long: Only " + MetadataSetOperation.VALUE_LENGTH_LIMIT + " bytes supported");
//        }
        // TODO handle CompletionException

        String previousValue = get(key);

        try {
            client.send(ClusterMetadataReplicationClient.createSetOperationMessage(scope, key, value)).join();
        } catch (CompletionException e) {
            // throw (RaftException) e.getCause();
        }

        return previousValue;
    }

    public String remove(String key) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        String oldValue = get(key);
        client.send(ClusterMetadataReplicationClient.createDeleteOperationMessage(scope, key)).join();
        return oldValue;
    }

    public void putAll(Map<String, String> m) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        for (val entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public void clear() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        // TODO use dedicated message
        fetchCurrentData();
        for (val key : currentData.keySet()) {
            remove(key);
        }
    }

    public Set keySet() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        fetchCurrentData();
        return currentData.keySet();
    }

    public Collection values() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        fetchCurrentData();
        return currentData.values();
    }

    public Set<Map.Entry<String, String>> entrySet() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        fetchCurrentData();
        return currentData.entrySet();
    }

    @SneakyThrows
    @Override
    public String toString() {
        fetchCurrentData();
        return currentData.toString();
    }
}
