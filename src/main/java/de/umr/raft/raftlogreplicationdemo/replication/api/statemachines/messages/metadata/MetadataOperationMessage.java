package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.OperationExecutionResult;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata.MetadataQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.messages.ExecutableMessage;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.MetadataOperationProto;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

// TODO may parametrize return proto type?
@RequiredArgsConstructor(staticName = "of")
public class MetadataOperationMessage implements ExecutableMessage<Map<String, Map<String, String>>, MetadataOperationResultProto> {

    @Getter private final MetadataOperationProto metaDataOperation;

    public static MetadataOperationMessage of(ByteString bytes) throws InvalidProtocolBufferException {
        return MetadataOperationMessage.of(MetadataOperationProto.parseFrom(bytes));
    }

    @Override
    public org.apache.ratis.thirdparty.com.google.protobuf.ByteString getContent() {
        return metaDataOperation.toByteString();
    }

    // TODO may implement isValid and check message schema

    @Override
    public boolean isTransactionMessage() {
        switch (metaDataOperation.getOperationType()) {
            case SET:
            case DELETE:
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return metaDataOperation.toString();
    }

    @Override
    public CompletableFuture<MetadataOperationResultProto> apply(Map<String, Map<String, String>> metadata) {
        return MetadataOperationExecutor.of(metaDataOperation).apply(metadata);
    }

    @Override
    public CompletableFuture<MetadataOperationResultProto> cancel() {
        return MetadataOperationExecutor.of(metaDataOperation).cancel();
    }

    public static class Factory {
        public static MetadataOperationMessage createDeleteOperationMessage(String scopeId, String key) {
            val metadataOperation = MetadataOperationProto.newBuilder()
                    .setOperationType(MetadataOperationType.DELETE)
                    .setScopeId(scopeId)
                    .setKey(key)
                    .build();

            return MetadataOperationMessage.of(metadataOperation);
        }

        public static MetadataOperationMessage createSetOperationMessage(String scopeId, String key, String value) {
            val metadataOperation = MetadataOperationProto.newBuilder()
                    .setOperationType(MetadataOperationType.SET)
                    .setScopeId(scopeId)
                    .setKey(key)
                    .setValue(value)
                    .build();

            return MetadataOperationMessage.of(metadataOperation);
        }

        public static MetadataOperationMessage createGetOperationMessage(String scopeId, String key) {
            val metadataOperation = MetadataOperationProto.newBuilder()
                    .setOperationType(MetadataOperationType.GET)
                    .setScopeId(scopeId)
                    .setKey(key)
                    .build();

            return MetadataOperationMessage.of(metadataOperation);
        }

        public static MetadataOperationMessage createGetAllForScopeOperationMessage(String scopeId) {
            val metadataOperation = MetadataOperationProto.newBuilder()
                    .setOperationType(MetadataOperationType.GET_ALL_FOR_SCOPE)
                    .setScopeId(scopeId)
                    .build();

            return MetadataOperationMessage.of(metadataOperation);
        }

        public static MetadataOperationMessage createGetAllOperationMessage() {
            val metadataOperation = MetadataOperationProto.newBuilder()
                    .setOperationType(MetadataOperationType.GET_ALL)
                    .build();

            return MetadataOperationMessage.of(metadataOperation);
        }

        public static MetadataOperationMessage createNullOperationMessage() {
            val metadataOperation = MetadataOperationProto.newBuilder()
                    .setOperationType(MetadataOperationType.NULL)
                    .build();

            return MetadataOperationMessage.of(metadataOperation);
        }
    }
}
