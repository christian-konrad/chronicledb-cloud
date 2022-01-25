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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    // TODO unit tests

    // TODO may further optimize it using an Optimistic Lock-Free Queue with simple CAS of the tail on flush.
    // TODO have proper graceful shutdown so all events will be flushed OR serialized to disk for later flush on server shutdown

    public static final long DEFAULT_CAPACITY = 102400; // 10kb
    public static final int DEFAULT_TIMEOUT = 1000; // 1 sec
    @Builder.Default @Getter @NonNull long capacityInBytes = DEFAULT_CAPACITY;
    @Getter private final AtomicLong size = new AtomicLong(0); // current filled size of the buffer in bytes
    @Builder.Default @Getter @NonNull final int timeOutMillis = DEFAULT_TIMEOUT;

    @NonNull final ConcurrentLinkedQueue<Event> eventQueue = new ConcurrentLinkedQueue<>();

    @NonNull final Function<List<Event>, Long> onFlush;
    @Builder.Default @NonNull final boolean sortBeforeFlush = false;

    @Getter @NonNull private final EventSerializer eventSerializer;

    private static final String THREAD_POOL_ID = "EventBufferThreadPool";

    private ResettableTimer timer;

    private final Logger LOG = LoggerFactory.getLogger(EventBuffer.class);

    public long getRemainingCapacity() {
        return capacityInBytes - size.get();
    }

    /**
     * Drains the internal queue and returns the elements as a list in the order of the queue
     * at the moment of draining.
     * As the iterator of the ConcurrentLinkedQueue is used, the drained elements are weakly consistent.
     * @return List of the events in the queue at the moment of draining
     */
    // TODO not that efficient: should better use a Optimistic Lock-Free Queue with simple CAS
    //  of the tail on flush or similar
    private synchronized List<Event> drain() {
        synchronized (eventQueue) {
            LOG.info("Draining the buffers queue");
            List<Event> clonedEventList = new ArrayList<>(eventQueue);
            eventQueue.clear();
            size.set(0);
            return clonedEventList;
        }
    }

    /**
     * Flushes the buffer by running the given onFlush callback.
     * The buffer is ready for further inserts as soon as the elements of the internal queue
     * are passed to the callback, it does not wait for the callback operation to be done.
     * Until then, the buffer is blocked.
     * @return A future that is completed once all events have
     * been flushed, returning the number of flushed events
     */
    public CompletableFuture<Long> flush() throws IOException {
        LOG.info("Attempting to flush the buffer");

        // if empty, nothing to flush
        if (eventQueue.isEmpty()) {
            LOG.info("TEST: Buffered insert stopped at " + System.currentTimeMillis());
            return CompletableFuture.completedFuture(0L);
        }

        timer.reset(false);

        // drain the events so the queue can continue receiving new events
        List<Event> events = drain();
        LOG.info("Buffers queue drained");

        // run the callback on the events
        return FutureUtil.wrapInCompletableFuture(() -> {
            // Sort events here before applying.
            // As .sort is stable, if events are already sorted operation is cheap (O(n))
            // We could synchronize over an atomic bool "shouldSort" and a lastInserted timestamp, but this would synchronize
            // the whole buffer which would be counterintuitive and result in synchronizing all inserts.
            // This may be significantly slower than just always sorting on flush (TODO check if assumption is true)
            if (sortBeforeFlush) {
                events.sort(Comparator.comparingLong(Event::getT1));
            }
            Long flushedEventsCount = onFlush.apply(events);
            LOG.info("Buffer flushed with {} events", flushedEventsCount);
            return flushedEventsCount;
        }, THREAD_POOL_ID);
    }

    // TODO check if the bulk-insert raft messages are received in-order.

    /**
     * Inserts a single event into this buffer in a thread-safe manner.
     * This results in adding the event to the internal ConcurrentLinkedQueue
     * as the new tail node.
     * If the operation would result in an overflow of the buffer's capacity,
     * the buffer is flushed beforehand.
     * While the buffer is passing its events to the onFlush callback when flushing,
     * it is blocking.
     * @param event
     * @throws IOException
     * @throws InterruptedException
     */
    public void insert(Event event) throws IOException, InterruptedException {
        timer.reset(false);

        // as we serialize here but do not use the serialized event, we could store it
        // and use it later in messaging to improve performance and reduce redundancy
        int estimatedEventSize = eventSerializer.getEstimatedEventSize(event);

        // decrease capacity
        if (capacityInBytes - size.addAndGet(estimatedEventSize) < 0) {
            LOG.info("Buffer full");
            // if left capacity is negative, flush (before event inserted!)
            // do not wait for flush callback to finish.
            // The queue automatically blocks as long as elements are drained
            flush();
        }

        eventQueue.add(event);
    }

    /**
     * Uses custom builder class
     */
    public static EventBufferBuilder builder() {
        return new EventBufferBuilder() {
            final Logger LOG = LoggerFactory.getLogger(EventBuffer.class);

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
                return eventBuffer;
            }
        };
    }
}
