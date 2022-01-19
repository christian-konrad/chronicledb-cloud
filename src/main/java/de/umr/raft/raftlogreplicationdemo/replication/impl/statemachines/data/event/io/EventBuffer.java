package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io;

import de.umr.event.Event;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.BufferedReplicatedEventStore;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import de.umr.raft.raftlogreplicationdemo.util.FutureUtil;
import de.umr.raft.raftlogreplicationdemo.util.scheduler.ResettableTimer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Builder
/**
 * Specific buffer implementation to hold and buffer Events
 * to be inserted in a ReplicatedEventStore in a batch manner.
 * Does implement an insert and a method to flush all of its content
 * into the EventStore or similar.
 * On instantiation, pass a certain capacity in bytes (not event count
 * as events with string contents can differ a lot in size) and a timeout
 * to flush content after the last insert.
 */
public class EventBuffer {

    public static final long DEFAULT_CAPACITY = 102400; // 10kb
    public static final int DEFAULT_TIMEOUT = 1000; // 1 sec
    @Builder.Default @Getter @NonNull long capacityInBytes = DEFAULT_CAPACITY;
    @Getter private final AtomicLong size = new AtomicLong(0); // current filled size of the buffer in bytes
    @Builder.Default @Getter @NonNull final int timeOutMillis = DEFAULT_TIMEOUT;
    @NonNull final List<Event> events = new ArrayList<>();
    @NonNull final Function<List<Event>, Long> onFlush;
    @Getter @NonNull private final EventSerializer eventSerializer;
    @Getter private final AtomicBoolean isFlushing = new AtomicBoolean(false);
    // THREE (implicit) states: READY, INSERTING and FLUSHING
    // READY -[insert event]-> INSERTING
    // INSERTING -[full]-> FLUSHING
    // INSERTING -[not full]-> READY
    // FLUSHING -[flushed]-> READY

    private static final String THREAD_POOL_ID = "EventBufferThreadPool";

    private ResettableTimer timer;

    private final Logger LOG = LoggerFactory.getLogger(EventBuffer.class);

    /**
     *
     * @return A future that is completed once all events have
     * been flushed, returning the number of flushed events
     */
    public synchronized CompletableFuture<Long> flush() throws IOException {
        LOG.info("Attempting to flush the buffer");

        synchronized (events) {
            // if empty, nothing to flush
            if (events.isEmpty()) return CompletableFuture.completedFuture(0L);

            synchronized (isFlushing) {
                if (isFlushing.get()) throw new IOException("Event buffer already flushing"); // TODO dedicated exception
                isFlushing.set(true);
            }

            // clone list as original list is mutated by original thread
            List<Event> clonedEventList = new ArrayList<>(events);

            CompletableFuture<Long> flushFuture = FutureUtil.wrapInCompletableFuture(() -> {
                synchronized (isFlushing) {
                    Long flushedEventsCount = onFlush.apply(clonedEventList);
                    isFlushing.set(false);
                    timer.reset(false);
                    LOG.info("Buffer flushed with {} events", flushedEventsCount);
                    return flushedEventsCount;
                }
            }, THREAD_POOL_ID);

            events.clear();
            size.set(0);

            return flushFuture;
        }
    }

    public long getRemainingCapacity() {
        return capacityInBytes - size.get();
    }

    public void insert(Event event) throws IOException, InterruptedException {
        synchronized (events) {
            // TODO is checking for flushing needed as we already synchronize by events?
            while (isFlushing.get()) {
                // TODO better wait (having a certain timeout so one won't wait forever)
                // TODO check if this is actually thrown or if waiting is not necessary
                // throw new IOException("Event buffer is flushing");
                wait(100);
            }

            timer.reset(false);

            // as we serialize here but do not use the serialized event, we could store it
            // and use it later in messaging to improve performance and reduce redundancy
            int estimatedEventSize = eventSerializer.getEstimatedEventSize(event);

            // decrease capacity
            if (capacityInBytes - size.addAndGet(estimatedEventSize) < 0) {
                LOG.info("Buffer full");
                // if left capacity is negative, flush (before event inserted!) and wait for flush before insert
                // TODO does this work while synchronizing over events list?
                flush().join();
            }

            events.add(event);
        }
    }



    /**
     * Uses custom builder class
     */
    public static EventBufferBuilder builder() {
        return new EventBufferBuilder() {
            Logger LOG = LoggerFactory.getLogger(EventBuffer.class);

            @Override
            public EventBuffer build() {
                EventBuffer eventBuffer = super.build();
                eventBuffer.timer = new ResettableTimer(() -> {
                    try {
                        LOG.info("Event buffer timeout");
                        eventBuffer.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, eventBuffer.timeOutMillis, TimeUnit.MILLISECONDS);
                eventBuffer.isFlushing.set(false);
                return eventBuffer;
            }
        };
    }
}
