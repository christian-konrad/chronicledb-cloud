package de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.facades;


import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata.MetadataOperationMessage;
import de.umr.raft.raftlogreplicationdemo.persistence.replication.impl.statemachines.messages.metadata.MetadataSetOperation;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.util.SerializationUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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
        if (scope.getBytes(MetadataOperationMessage.UTF8).length > MetadataSetOperation.NODE_ID_LENGTH_LIMIT) {
            throw new IllegalArgumentException("Node id too long: Only " + MetadataSetOperation.NODE_ID_LENGTH_LIMIT + " bytes supported");
        }
        this.scope = scope;
        this.client = client;
    }

    public static ReplicatedMetadataMap of(String scope, ClusterMetadataReplicationClient client) {
        return new ReplicatedMetadataMap(scope, client);
    }

    public static ReplicatedMetadataMap ofRaftGroupScope(ClusterMetadataReplicationClient client) {
        return ReplicatedMetadataMap.of("raft-groups", client);
    }

    private void fetchCurrentData() throws ExecutionException, InterruptedException {
        val response = client.sendReadOnly(ClusterMetadataReplicationClient.GET_MESSAGE).get().getMessage();
        val data = (ConcurrentHashMap<String, Map<String, String>>) SerializationUtils.deserialize(response.getContent().toByteArray());
        currentData = data.get(scope);
        if (currentData == null) {
            currentData = new HashMap<>();
        }
    }

    public boolean isEmpty() throws ExecutionException, InterruptedException {
        fetchCurrentData();
        return currentData.isEmpty();
    }

    public boolean containsKey(String key) throws ExecutionException, InterruptedException {
        fetchCurrentData();
        return currentData.containsKey(key);
    }

    public String get(String key) throws ExecutionException, InterruptedException {
        fetchCurrentData();
        return currentData.get(key);
    }

    public String put(String key, String value) throws ExecutionException, InterruptedException {
        // TODO receive message for success state
        if (key.getBytes(MetadataOperationMessage.UTF8).length > MetadataSetOperation.KEY_LENGTH_LIMIT) {
            throw new IllegalArgumentException("Key too long: Only " + MetadataSetOperation.KEY_LENGTH_LIMIT + " bytes supported");
        }
        if (value.getBytes(MetadataOperationMessage.UTF8).length > MetadataSetOperation.VALUE_LENGTH_LIMIT) {
            throw new IllegalArgumentException("Value too long: Only " + MetadataSetOperation.VALUE_LENGTH_LIMIT + " bytes supported");
        }
        client.send(ClusterMetadataReplicationClient.createSetOperationMessage(scope, key, value)).join();
        return get(key);
    }

    public String remove(String key) throws ExecutionException, InterruptedException {
        String oldValue = get(key);
        client.send(ClusterMetadataReplicationClient.createDeleteOperationMessage(scope, key)).join();
        return oldValue;
    }

    public void putAll(Map<String, String> m) throws ExecutionException, InterruptedException {
        for (val entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public void clear() throws ExecutionException, InterruptedException {
        fetchCurrentData();
        for (val key : currentData.keySet()) {
            remove(key);
        }
    }

    public Set keySet() throws ExecutionException, InterruptedException {
        fetchCurrentData();
        return currentData.keySet();
    }

    public Collection values() throws ExecutionException, InterruptedException {
        fetchCurrentData();
        return currentData.values();
    }

    public Set<Map.Entry<String, String>> entrySet() throws ExecutionException, InterruptedException {
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
