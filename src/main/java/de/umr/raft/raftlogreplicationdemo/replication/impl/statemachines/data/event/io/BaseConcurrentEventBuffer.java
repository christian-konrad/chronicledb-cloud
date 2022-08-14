package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io;

import de.umr.event.Event;
import de.umr.raft.raftlogreplicationdemo.replication.api.statemachines.data.event.io.EventBuffer;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import de.umr.raft.raftlogreplicationdemo.util.scheduler.ResettableTimer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Specific buffer implementation to hold and buffer Events
 * to be inserted in a ReplicatedEventStore in a batch manner.
 * Does implement an insert and a method to flush all of its content
 * into the EventStore or similar.
 * On instantiation, pass a certain capacity in bytes (not event count
 * as events with string contents can differ a lot in size) and a timeout
 * to flush content after the last insert.
 */
public abstract class BaseConcurrentEventBuffer implements EventBuffer {

    // TODO unit tests

    // TODO may further optimize it using an Optimistic Lock-Free Queue with simple CAS of the tail on flush.
    // TODO have proper graceful shutdown so all events will be flushed OR serialized to disk for later flush on server shutdown

    public static final long DEFAULT_CAPACITY = 102400; // 10kb
    public static final int DEFAULT_TIMEOUT = 1000; // 1 sec
    @Getter @NonNull long capacityInBytes;
    @Getter @NonNull final int timeOutMillis;
    protected final AtomicLong size = new AtomicLong(0); // current filled size of the buffer in bytes

    @NonNull final Function<List<Event>, Long> onFlush;
    @NonNull final boolean sortBeforeFlush;

    @NonNull private final EventSerializer eventSerializer;

    protected ResettableTimer timer;

    protected final Logger LOG = LoggerFactory.getLogger(BaseConcurrentEventBuffer.class);

    public long getRemainingCapacity() {
        return capacityInBytes - size.get();
    }

    // TODO check and compare performance of ConcurrentLinkedQueue and simple, single-threaded queues
    // TODO check if pre-serializing to check for size slows it down. Check if capacity in count of events is sufficient

    /**
     * Drains the internal queue and returns the elements as a list in the order of the queue
     * at the moment of draining.
     * As the iterator of the ConcurrentLinkedQueue is used, the drained elements are weakly consistent.
     * @return List of the events in the queue at the moment of draining
     */
    protected abstract List<Event> drainQueue();

    protected void incrementCount() {
        // noop
    }

    protected abstract boolean isQueueEmpty();

    protected abstract void addEventToQueue(Event event);

    protected abstract void clearQueue();

    protected void resetTimer(boolean mayInterruptIfRunning) {
        timer.reset(mayInterruptIfRunning);
    };

    /**
     * Flushes the buffer by running the given onFlush callback.
     * The buffer is ready for further inserts as soon as the elements of the internal queue
     * are passed to the callback, it does not wait for the callback operation to be done.
     * Until then, the buffer is blocked.
     * @return A future that is completed once all events have
     * been flushed, returning the number of flushed events
     */
    public CompletableFuture<Long> flush() throws IOException {
        LOG.debug("Attempting to flush the buffer");

        // if empty, nothing to flush
        if (isQueueEmpty()) {
            return CompletableFuture.completedFuture(0L);
        }

        resetTimer(false);

        // drain the events so the queue can continue receiving new events
        List<Event> events = drainQueue();
        LOG.debug("Buffer queue drained");

        // Sort events here before applying the callback.
        // As .sort is stable, if events are already sorted operation is cheap (O(n))
        // We could synchronize over an atomic bool "shouldSort" and a lastInserted timestamp, but this would synchronize
        // the whole buffer which would be counterintuitive and result in synchronizing all inserts.
        // This may be significantly slower than just always sorting on flush (TODO check if assumption is true)
        if (sortBeforeFlush) {
            events.sort(Comparator.comparingLong(Event::getT1));
        }

        // run the callback on the events
        Long flushedEventsCount = onFlush.apply(events);
        LOG.info("Buffer flushed with {} events", flushedEventsCount);

        return CompletableFuture.completedFuture(flushedEventsCount);
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
        resetTimer(false);

        // as we serialize here but do not use the serialized event, we could store it
        // and use it later in messaging to improve performance and reduce redundancy

        // TODO just use event count, not event bytes for the buffer. State out in thesis that the buffer must be in bytes accordingly to the raft log buffer max size
        int estimatedEventSize = eventSerializer.getEstimatedEventSize(event);

        addEventToQueue(event);

        // decrease capacity
        incrementCount();
        if (capacityInBytes - size.addAndGet(estimatedEventSize) < 0) {
            LOG.debug("Buffer full");
            // if left capacity is negative, flush (as buffer is full).
            // do not wait for flush callback to finish.
            // The queue automatically blocks as long as elements are drained
            flush();
        }
    }

    @Override
    public void clear() {
        resetTimer(true);
        clearQueue();
        size.set(0);
    }

    private void initTimer() {
        timer = new ResettableTimer(() -> {
            try {
                LOG.debug("Event buffer timeout");
                flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, timeOutMillis, TimeUnit.MILLISECONDS);
    }

    protected BaseConcurrentEventBuffer(long capacityInBytes, int timeOutMillis, EventSerializer eventSerializer, Function<List<Event>, Long> onFlush, boolean sortBeforeFlush) {
        this.capacityInBytes = capacityInBytes;
        this.timeOutMillis = timeOutMillis;
        this.eventSerializer = eventSerializer;
        this.onFlush = onFlush;
        this.sortBeforeFlush = sortBeforeFlush;

        initTimer();
    }

    protected BaseConcurrentEventBuffer(EventSerializer eventSerializer, Function<List<Event>, Long> onFlush, boolean sortBeforeFlush) {
        this.capacityInBytes = DEFAULT_CAPACITY;
        this.timeOutMillis = DEFAULT_TIMEOUT;
        this.eventSerializer = eventSerializer;
        this.onFlush = onFlush;
        this.sortBeforeFlush = sortBeforeFlush;

        initTimer();
    }

    protected BaseConcurrentEventBuffer(EventSerializer eventSerializer, Function<List<Event>, Long> onFlush) {
        this.capacityInBytes = DEFAULT_CAPACITY;
        this.timeOutMillis = DEFAULT_TIMEOUT;
        this.eventSerializer = eventSerializer;
        this.onFlush = onFlush;
        this.sortBeforeFlush = false;

        initTimer();
    }
}
