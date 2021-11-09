package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.chronicledb.common.query.cursor.Cursor;
import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.AggregatedEventStore;
import de.umr.chronicledb.event.store.EventStore;
import de.umr.chronicledb.event.store.tabPlus.aggregation.EventAggregationValues;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.global.EventCount;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.EventStoreReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.DemoEventSchemaProvider;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.FinalizedEventAggregationValues;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.AggregateRequestSerializer;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ReplicatedEventStore implements AggregatedEventStore {

    private final EventStoreReplicationClient client;
    @Getter private final String streamName;

    Logger LOG = LoggerFactory.getLogger(ReplicatedEventStore.class);

    @Override
    public EventSchema getSchema() {
        // TODO implement via GetSchemaOperationMessage
        return new DemoEventSchemaProvider().getSchema("DEMO");
    }

    @Override
    public Optional<Range<Long>> getKeyRange() {
        EventStoreOperationResultProto responseProto = null;
        try {
            responseProto = client.sendAndExecuteOperationMessage(
                    EventStoreReplicationClient.createGetKeyRangeEventOperationMessage(),
                    EventStoreOperationResultProto.parser());
        } catch (Exception e) { // catch everything here to prevent sneaky throws from eating runtime errors
            LOG.error("Could not get key range", e);
            throw new UnsupportedOperationException("Could not get key range");
        }

        if (!responseProto.getStatus().equals(OperationResultStatus.OK)) {
            throw new UnsupportedOperationException();
        }

        if (responseProto.getIsNullResult()) {
            return Optional.empty();
        }

        val keyRangeResultProto = responseProto.getKeyRangeResult();
        val keyRange = AggregateRequestSerializer.fromProto(keyRangeResultProto);
        return Optional.of(keyRange);
    }

    @Override
    public Cursor<Event> filter(Range<Long> range, Predicate<Event> predicate) throws IllegalStateException, IOException {
        // TODO implement via FilterEventsOperationMessage
        throw new UnsupportedOperationException("Currently not implemented");

        // return null;
    }

    @Override
    public Cursor<Event> filter(Range<Long> range, Predicate<Event> predicate, Predicate<EventAggregationValues> predicate1) throws IllegalStateException, IOException {
        // TODO implement via FilterEventsOperationMessage
        throw new UnsupportedOperationException("Currently not implemented");

        // return null;
    }

    @Override
    public EventAggregationValues getAggregates(Range<Long> range, List<? extends EventAggregate> list) throws IllegalStateException, IOException {
        EventStoreOperationResultProto responseProto = null;
        try {
        responseProto = client.sendAndExecuteOperationMessage(
                EventStoreReplicationClient.createGetAggregatesEventOperationMessage(range, list),
                EventStoreOperationResultProto.parser());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new UnsupportedOperationException("Could not get aggregate");
        }

        if (!responseProto.getStatus().equals(OperationResultStatus.OK)
                || responseProto.getIsNullResult()
                || !responseProto.getOperationType().equals(EventStoreOperationType.AGGREGATE)) {
            // TODO throw more meaningful exception
            throw new UnsupportedOperationException();
        }

        // EventStoreOperationResultProto -> EventAggregationValues

        val resultProto = responseProto.getAggregationResultMap();

        Map<String, Optional<?>> resultMap = resultProto.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> AggregateRequestSerializer.fromProto(entry.getValue()))
        );

        return new FinalizedEventAggregationValues(resultMap);
    }

    public long eventCount() throws IOException {
        return getAggregate(new EventCount(), Long.class).orElse(0L);
    }

    @Override
    public void insert(Event event) throws IllegalStateException, IOException {
        // TODO must always obtain schema first; then cache schema
        EventStoreOperationResultProto responseProto = null;
        try {
            responseProto = client.sendAndExecuteOperationMessage(
                    EventStoreReplicationClient.createPushEventOperationMessage(event, getSchema()),
                    EventStoreOperationResultProto.parser());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new UnsupportedOperationException("Could not insert event");
        }

        if (!responseProto.getStatus().equals(OperationResultStatus.OK)) {
            // TODO throw more meaningful exception
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close() throws IOException {
        // TODO implement via CloseEventStoreOperationMessage
        // noop
    }

    @Override
    public boolean isOpen() {
        // TODO implement via OpenEventStoreOperationMessage
        return true;
    }

    private ReplicatedEventStore(String streamName, EventStoreReplicationClient client) {
        this.streamName = streamName;
        this.client = client;
    }

    public static ReplicatedEventStore of(String streamName, EventStoreReplicationClient client) {
        return new ReplicatedEventStore(streamName, client);
    }
}
