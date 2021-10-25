package de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.metadata;

import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.OperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.NullOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.metadata.*;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface MetadataOperationExecutor<ResultType extends Message> extends OperationExecutor<Map<String, Map<String, String>>, ResultType> {
    static MetadataOperationExecutor of(MetadataOperationProto metaDataOperation) {
        switch (metaDataOperation.getOperationType()) {
            case SET:
                return MetadataSetOperationExecutor.of(metaDataOperation);
            case DELETE:
                return MetadataDeleteOperationExecutor.of(metaDataOperation);
            case GET:
                return MetadataGetOperationExecutor.of(metaDataOperation);
            case GET_ALL_FOR_SCOPE:
                return MetadataGetAllForScopeOperationExecutor.of(metaDataOperation);
            case GET_ALL:
                return MetadataGetAllOperationExecutor.of(metaDataOperation);
            case NULL:
            case UNRECOGNIZED:
            default:
                return MetadataNullOperationExecutor.getInstance();
        }
    }

    MetadataOperationType getOperationType();

    @SuppressWarnings("unchecked")
    default ResultType createCancellationResponse(MetadataOperationType operationType) {
        return (ResultType) MetadataOperationResultProto.newBuilder()
                .setOperationType(operationType)
                .setStatus(OperationResultStatus.CANCELLED)
                .build();
    }

    @Override
    public default CompletableFuture<ResultType> cancel() {
        return CompletableFuture.completedFuture(createCancellationResponse(getOperationType()));
    }

    class MetadataNullOperationExecutor implements MetadataOperationExecutor<NullOperationResultProto>, NullOperationExecutor<Map<String, Map<String, String>>> {
        private static final MetadataNullOperationExecutor INSTANCE = new MetadataNullOperationExecutor();

        private MetadataNullOperationExecutor() {}

        static MetadataNullOperationExecutor getInstance() {
            return INSTANCE;
        }

        @Override
        public MetadataOperationType getOperationType() {
            return MetadataOperationType.NULL;
        }

        @Override
        public CompletableFuture<NullOperationResultProto> cancel() {
            return NullOperationExecutor.super.cancel();
        }
    }
}
