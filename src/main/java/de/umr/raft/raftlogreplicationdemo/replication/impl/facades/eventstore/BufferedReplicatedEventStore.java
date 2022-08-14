package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.event.Event;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.data.event.io.EventBuffer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.EventStoreReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io.BaseConcurrentEventBuffer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io.BlockingEventBuffer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io.ConcurrentEventBuffer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class BufferedReplicatedEventStore extends ReplicatedEventStore {

    private final EventBuffer eventBuffer;
    Logger LOG = LoggerFactory.getLogger(BufferedReplicatedEventStore.class);

    public void insertBuffered(Event event) throws IOException, InterruptedException {
        // TODO enforce that only the current leader is allowed to accept buffered writes!
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

    // TODO check if flushs can overlap or if inserts into this are overlapping
    private EventBuffer createEventBuffer(long bufferCapacity, int bufferTimeout) {
        val schema = getSchema();
        val eventSerializer = EventSerializer.of(schema);
        Function<List<Event>, Long> onFlush = (events) -> {
            try {
                val eventCount = events.stream().count();

                if (events.isEmpty()) {
                    // do nothing
                    return 0L;
                }

                if (LOG.isDebugEnabled()) {
                    val firstEvent = events.get(0);
                    val firstT1 = firstEvent.getT1();
                    val lastEvent = events.get(events.size() - 1);
                    val lastT1 = lastEvent.getT1();
                    LOG.debug("First T1 of flushed events: " + firstT1);
                    LOG.debug("Last T1 of flushed events: " + lastT1);
                }

                // the buffer sorts events before flushing, so we set ordered to true
                insert(events.iterator(), true);
                return eventCount;
            } catch (IOException e) {
                e.printStackTrace();
                return 0L;
            }
        };

        return BlockingEventBuffer.builder()
        //return ConcurrentEventBuffer.builder()
                .capacityInBytes(bufferCapacity)
                .timeOutMillis(bufferTimeout)
                .eventSerializer(eventSerializer)
                .onFlush(onFlush)
                .sortBeforeFlush(true)
                .build();
    }

    private EventBuffer createEventBuffer() {
        return createEventBuffer(BaseConcurrentEventBuffer.DEFAULT_CAPACITY, BaseConcurrentEventBuffer.DEFAULT_TIMEOUT);
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

    public void clear() throws IOException {
        // TODO clear buffer (should not flush its content anymore)
        this.eventBuffer.clear();
        super.clear();
    }
}
