package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io;

import de.umr.event.Event;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import de.umr.raft.raftlogreplicationdemo.util.scheduler.ResettableTimer;
import lombok.Builder;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Implementation of the concurrent event buffer based on a ConcurrentLinkedQueue.
 * Holds and buffers Events to be inserted in a ReplicatedEventStore in a batch manner.
 * Does implement an insert and a method to flush all of its content
 * into the EventStore or similar.
 * On instantiation, pass a certain capacity in bytes (not event count
 * as events with string contents can differ a lot in size) and a timeout
 * to flush content after the last insert.
 */
public class ConcurrentEventBuffer extends BaseConcurrentEventBuffer {

    @NonNull
    final ConcurrentLinkedQueue<Event> eventQueue = new ConcurrentLinkedQueue<>();
    protected final AtomicLong count = new AtomicLong(0); // current count of events in the buffer

    // TODO unit tests

    /**
     * Drains the internal queue and returns the elements as a list in the order of the queue
     * at the moment of draining.
     * As the iterator of the ConcurrentLinkedQueue is used, the drained elements are weakly consistent.
     * @return List of the events in the queue at the moment of draining
     */
    @Override
    protected synchronized List<Event> drainQueue() {
        synchronized (eventQueue) {
            LOG.debug("Draining the buffers queue");
            long eventsCount = count.getAndSet(0);
            size.set(0);

            List<Event> eventList = new ArrayList<>();
            // poll all elements from the queue (do not just clone and clear the queue as inserts are not synchronized!)

            // dont just poll until queue is empty as there still can be inserts
            for (long i = 0; i < eventsCount; i++) {
                Event event = eventQueue.poll();
                if (event == null) break;
                eventList.add(event);
            }
            return eventList;
        }
    }

    @Override
    protected void addEventToQueue(Event event) {
        eventQueue.add(event);
    }

    @Override
    protected void incrementCount() {
        count.incrementAndGet();
    }

    @Override
    protected boolean isQueueEmpty() {
        return eventQueue.isEmpty();
    }

    @Override
    protected void clearQueue() {
        eventQueue.clear();
    }

    @Builder
    public ConcurrentEventBuffer(long capacityInBytes, int timeOutMillis, EventSerializer eventSerializer, Function<List<Event>, Long> onFlush, boolean sortBeforeFlush) {
        super(capacityInBytes, timeOutMillis, eventSerializer, onFlush, sortBeforeFlush);
    }

    @Builder
    public ConcurrentEventBuffer(EventSerializer eventSerializer, Function<List<Event>, Long> onFlush, boolean sortBeforeFlush) {
        super(eventSerializer, onFlush, sortBeforeFlush);
    }

    @Builder
    public ConcurrentEventBuffer(EventSerializer eventSerializer, Function<List<Event>, Long> onFlush) {
        super(eventSerializer, onFlush);
    }
}
