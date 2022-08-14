package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.executors.eventstore;

import de.umr.chronicledb.event.store.tabPlus.aggregation.EventAggregationValues;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.*;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.executors.eventstore.EventStoreQueryOperationExecutor;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.EventStoreState;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.AggregateRequestSerializer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "of")
public class GetAggregatesOperationExecutor implements EventStoreQueryOperationExecutor {

    Logger LOG = LoggerFactory.getLogger(GetAggregatesOperationExecutor.class);

    @Getter
    private final EventStoreOperationProto eventStoreOperation;

    @Override
    public CompletableFuture<EventStoreOperationResultProto> apply(EventStoreState eventStoreState) {
        val aggregateRequestProto = eventStoreOperation.getAggregateRequest();

        val eventStore = eventStoreState.getEventStore();

        val rangeProto = aggregateRequestProto.getRange();
        val aggregateProtos = aggregateRequestProto.getEventAggregatesList();

        val range = AggregateRequestSerializer.fromProto(rangeProto);
        val aggregates = AggregateRequestSerializer.fromProto(aggregateProtos);

        EventAggregationValues eventAggregationValues = null;
        try {
            eventAggregationValues = eventStore.getAggregates(range, aggregates);
        } catch (Exception e) { // EPRuntimeException
            LOG.error("Error on getting aggregation values from tab+ store");
            e.printStackTrace();
            //val exception = new ExecutionException("Can not access the index to retrieve aggregates", e);
            return CompletableFuture.completedFuture(createUnsuccessfulOperationResult());
        }

        // retrieve all aggregates right now as later optimizer and compiler access in client is not possible
        val result = unwrapEventAggregationValues(eventAggregationValues, aggregates);

        return CompletableFuture.completedFuture(createEventStoreOperationResult(result));
    }


    private Map<String, Optional<?>> unwrapEventAggregationValues(EventAggregationValues eventAggregationValues, List<? extends EventAggregate> aggregates) {
        Map<String, Optional<?>> values = new HashMap<>();
        aggregates.forEach(aggregate -> {
            val aggregateInstanceId = aggregate.toString();
            val value = eventAggregationValues.getAggregationValue(aggregate);
            values.put(aggregateInstanceId, value);
        });
        return values;
    }

    private EventStoreOperationResultProto createEventStoreOperationResult(Map<String, Optional<?>> result) {
        val resultProto = result.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> createAggregationValueProto(entry.getValue()))
        );

        return EventStoreOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setStatus(OperationResultStatus.OK)
                .putAllAggregationResult(resultProto)
                .build();
    }

    private AggregationValueProto createAggregationValueProto(Optional<?> optional) {
        val protoBuilder = AggregationValueProto.newBuilder();

        if (optional.isEmpty()) {
            protoBuilder.setIsNull(true);
        } else {
            Object value = optional.get();

            // TODO do we have also primitives here?

            if (!(value instanceof Number)) {
                throw new UnsupportedOperationException("Currently, only primitive numerical aggregates are supported");
            }
            if (value instanceof Float) {
                protoBuilder.setFloatValue((Float) value).setValueType(AggregationValueType.FLOAT);
            } else if (value instanceof Double) {
                protoBuilder.setFloatValue(((Double) value).floatValue()).setValueType(AggregationValueType.FLOAT);
            } else if (value instanceof Long) {
                protoBuilder.setLongValue((Long) value).setValueType(AggregationValueType.LONG);
            } else if (value instanceof Integer) {
                protoBuilder.setLongValue(((Integer) value).longValue()).setValueType(AggregationValueType.LONG);
            } else if (value instanceof Short) {
                protoBuilder.setLongValue(((Short) value).longValue()).setValueType(AggregationValueType.LONG);
            } else {
                throw new UnsupportedOperationException("Unsupported aggregate value type");
            }
        }

        return protoBuilder.build();
    }

    private EventStoreOperationResultProto createUnsuccessfulOperationResult() {
        return EventStoreOperationResultProto.newBuilder()
                .setOperationType(getOperationType())
                .setStatus(OperationResultStatus.ERROR)
                .build();
    }

    @Override
    public EventStoreOperationType getOperationType() {
        return EventStoreOperationType.AGGREGATE;
    }
}
