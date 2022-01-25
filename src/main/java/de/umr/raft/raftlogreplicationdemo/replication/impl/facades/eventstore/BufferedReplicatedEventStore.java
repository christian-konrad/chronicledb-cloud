package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.chronicledb.common.query.cursor.Cursor;
import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.AggregatedEventStore;
import de.umr.chronicledb.event.store.tabPlus.aggregation.EventAggregationValues;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.global.EventCount;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationResultProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventStoreOperationType;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.OperationResultStatus;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.EventStoreReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.DemoEventSchemaProvider;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.FinalizedEventAggregationValues;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io.EventBuffer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.AggregateRequestSerializer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import lombok.Getter;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BufferedReplicatedEventStore extends ReplicatedEventStore {

    private final EventBuffer eventBuffer;
    Logger LOG = LoggerFactory.getLogger(BufferedReplicatedEventStore.class);

    // TODO future?
    public void insertBuffered(Event event) throws IOException, InterruptedException {
        eventBuffer.insert(event);
    }


    @Override
    public void close() throws IOException {
        try {
            eventBuffer.flush().join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            super.close();
        }
    }

    private EventBuffer createEventBuffer(long bufferCapacity, int bufferTimeout) {
        val schema = getSchema();
        val eventSerializer = EventSerializer.of(schema);
        Function<List<Event>, Long> onFlush = (events) -> {
            try {
                val eventCount = events.stream().count();
                // the buffer sorts events before flushing, so we set ordered to true
                insert(events.iterator(), true);
                return eventCount;
            } catch (IOException e) {
                e.printStackTrace();
                return 0L;
            }
        };
        return EventBuffer.builder()
                .capacityInBytes(bufferCapacity)
                .timeOutMillis(bufferTimeout)
                .eventSerializer(eventSerializer)
                .onFlush(onFlush)
                .sortBeforeFlush(true)
                .build();
    }

    private EventBuffer createEventBuffer() {
        return createEventBuffer(EventBuffer.DEFAULT_CAPACITY, EventBuffer.DEFAULT_TIMEOUT);
    }

    private BufferedReplicatedEventStore(String streamName, EventStoreReplicationClient client) {
        super(streamName, client);
        this.eventBuffer = createEventBuffer();
    }

    public static BufferedReplicatedEventStore of(String streamName, EventStoreReplicationClient client) {
        return new BufferedReplicatedEventStore(streamName, client);
    }

    private BufferedReplicatedEventStore(String streamName, EventStoreReplicationClient client, long bufferCapacity, int bufferTimeout) {
        super(streamName, client);
        this.eventBuffer = createEventBuffer(bufferCapacity, bufferTimeout);
    }

    public static BufferedReplicatedEventStore of(String streamName, EventStoreReplicationClient client, long bufferCapacity, int bufferTimeout) {
        return new BufferedReplicatedEventStore(streamName, client, bufferCapacity, bufferTimeout);
    }
}
