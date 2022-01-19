package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.metadata;


import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultMapProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.ratis.protocol.RaftGroupId;
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

    public static ReplicatedMetadataMap ofRaftGroupRegistry(ClusterMetadataReplicationClient client) {
        return ReplicatedMetadataMap.of("raft-groups", client);
    }

    /**
     * Removes "group-" prefix of group IDs as otherwise,
     * the key would be raft-group-group-<ID>
     * @param raftGroupId Id of the raft group
     * @return The id of the group without leading "group-" prefix
     */
    private static String prepareGroupId(String raftGroupId) {
        return raftGroupId.replace("group-", "");
    }

    public static ReplicatedMetadataMap ofRaftGroupScope(String raftGroupId, ClusterMetadataReplicationClient client) {
        return ReplicatedMetadataMap.of("raft-group-" + prepareGroupId(raftGroupId), client);
    }

    public static ReplicatedMetadataMap ofRaftGroupScope(RaftGroupId raftGroupId, ClusterMetadataReplicationClient client) {
        return ReplicatedMetadataMap.ofRaftGroupScope(raftGroupId.toString(), client);
    }

    public static ReplicatedMetadataMap ofDivision(String divisionId, ClusterMetadataReplicationClient client) {
        return ReplicatedMetadataMap.of("division-" + divisionId, client);
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
    }

    private String put(String key, String value, Boolean async) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        String previousValue;
        try {
            previousValue = get(key);
        } catch (NoSuchElementException e) {
            previousValue = null;
        }

        try {
            val message = ClusterMetadataReplicationClient.createSetOperationMessage(scope, key, value);
            val future = client.send(message);
            if (!async) future.join();
        } catch (CompletionException e) { // TODO handle
            // throw (RaftException) e.getCause();
        }

        return previousValue;
    }

    public String put(String key, String value) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        return put(key, value, false);
    }

    public String putAsync(String key, String value) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        return put(key, value, true);
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

    private static Object parseResultMapProto(MetadataOperationResultMapProto resultMapProto) {
        if (resultMapProto.getIsNull()) {
            return null;
        }

        if (resultMapProto.getIsLeaf()) {
            return resultMapProto.getLeafValue();
        }

        return resultMapProto.getNodesMap().entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(
                        entry.getKey(),
                        parseResultMapProto(entry.getValue())))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    public static Map<String, Map<String, String>> getFullMetaDataMap(ClusterMetadataReplicationClient client) throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        val responseProto = client.sendAndExecuteOperationMessage(
                ClusterMetadataReplicationClient.createGetAllOperationMessage(),
                MetadataOperationResultProto.parser());

        if (!responseProto.getStatus().equals(OperationResultStatus.OK)) {
            // TODO throw exception
            return null;
        }

        val resultMapProto = responseProto.getResult();

        if (resultMapProto.getIsNull()) {
            return new HashMap<>();
        }

        return (Map<String, Map<String, String>>) parseResultMapProto(resultMapProto);

//        return resultMapProto.getNodesMap().entrySet().stream()
//                .map(entry -> new AbstractMap.SimpleEntry<>(
//                        entry.getKey(),
//                        entry.getValue().getLeafValue()))
//                .collect(Collectors.toMap(
//                        Map.Entry::getKey,
//                        Map.Entry::getValue
//                ));

    }

    @SneakyThrows
    @Override
    public String toString() {
        fetchCurrentData();
        return currentData.toString();
    }
}
