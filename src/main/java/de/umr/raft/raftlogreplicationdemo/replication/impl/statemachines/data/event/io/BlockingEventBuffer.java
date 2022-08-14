package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.io;

import de.umr.event.Event;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization.EventSerializer;
import lombok.Builder;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * Blocking implementation of the concurrent event buffer based on a LinkedBlockingQueue.
 * Holds and buffers Events to be inserted in a ReplicatedEventStore in a batch manner.
 * Does implement an insert and a method to flush all of its content
 * into the EventStore or similar.
 * On instantiation, pass a certain capacity in bytes (not event count
 * as events with string contents can differ a lot in size) and a timeout
 * to flush content after the last insert.
 */
public class BlockingEventBuffer extends BaseConcurrentEventBuffer {

    @NonNull
    final LinkedBlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

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
            size.set(0);

            List<Event> eventList = new ArrayList<>();
            eventQueue.drainTo(eventList);

            return eventList;
        }
    }

    @Override
    protected void addEventToQueue(Event event) {
        // TODO should use put and restrict queue to capacity (but then buffer capacity should be events, not kb)
        eventQueue.add(event);
    }

    @Override
    protected void clearQueue() {
        eventQueue.clear();
    }

    @Override
    protected boolean isQueueEmpty() {
        return eventQueue.isEmpty();
    }

    @Builder
    public BlockingEventBuffer(long capacityInBytes, int timeOutMillis, EventSerializer eventSerializer, Function<List<Event>, Long> onFlush, boolean sortBeforeFlush) {
        super(capacityInBytes, timeOutMillis, eventSerializer, onFlush, sortBeforeFlush);
    }

    @Builder
    public BlockingEventBuffer(EventSerializer eventSerializer, Function<List<Event>, Long> onFlush, boolean sortBeforeFlush) {
        super(eventSerializer, onFlush, sortBeforeFlush);
    }

    @Builder
    public BlockingEventBuffer(EventSerializer eventSerializer, Function<List<Event>, Long> onFlush) {
        super(eventSerializer, onFlush);
    }
}
