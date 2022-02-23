package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.AggregatedEventStore;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.global.EventCount;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.event.schema.SchemaProvider;
import de.umr.jepc.v2.api.epa.EPA;
import de.umr.lambda.jepc.ql.QueryCompiler;
import de.umr.lambda.ql.lang.EPQueryCompiler;
import de.umr.lambda.ql.operators.LogicalOperator;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.ClearStreamRequest;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryRequest;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryResponse;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamInfo;
import lombok.SneakyThrows;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public abstract class BaseChronicleEngine<EventStoreImpl extends AggregatedEventStore> implements SchemaProvider, AutoCloseable {

    protected final EventStoreProvider eventStoreProvider;
    protected final ClearStreamRequestProvider clearStreamRequestProvider;

    private boolean eventStoreProviderInitialized = false;

    public BaseChronicleEngine() {
        eventStoreProvider = new EventStoreProvider();
        clearStreamRequestProvider = new ClearStreamRequestProvider();
    }

    protected abstract void initStreams() throws IOException;

    @SneakyThrows
    protected void initEventStoreProvider() {
        if (eventStoreProviderInitialized) return;
        initStreams();
        eventStoreProviderInitialized = true;
    }

    protected EventStoreProvider getEventStoreProvider() {
        if (!eventStoreProviderInitialized) initEventStoreProvider();
        return eventStoreProvider;
    }

    public List<String> getStreamNames() throws IOException, ExecutionException, InterruptedException {
        return getEventStoreProvider().eventStores.keySet().stream().sorted().collect(Collectors.toList());
    }

    public long eventCount(String streamName) throws IOException {
        EventStoreImpl eventStore = getEventStoreProvider().get(streamName);
        return eventStore.getAggregate(new EventCount(), Long.class).orElse(0L);
    }

    public StreamInfo getStreamInfo(String streamName) throws IOException {
        EventStoreImpl eventStore = getEventStoreProvider().get(streamName);
        return new StreamInfo(streamName, eventCount(streamName), eventStore.getKeyRange().orElse(null), eventStore.getSchema());
    }

    public Optional<?> getAggregate(String streamName, Range<Long> keyRange, EventAggregate aggregate)
            throws IllegalStateException, IOException {
        EventStoreImpl eventStore = getEventStoreProvider().get(streamName);
        return eventStore.getAggregate(keyRange, aggregate);
    }

    public Optional<?> getAggregate(String streamName, EventAggregate aggregate)
            throws IllegalStateException, IOException {
        EventStoreImpl eventStore = getEventStoreProvider().get(streamName);
        return eventStore.getAggregate(aggregate);
    }

    public abstract EventStoreImpl registerStream(String name, EventSchema schema) throws IOException;

    public abstract void pushEvent(String stream, Event event) throws IOException, InterruptedException;

    public abstract void removeStream(String name) throws IOException;

    @Override
    public EventSchema getSchema(String stream) {
        return getEventStoreProvider().get(stream).getSchema();
    }

    public EPA translateQuery(String queryString) throws ParseException {
        LogicalOperator logicalOperator = EPQueryCompiler.compileQuery(queryString, this);
        QueryCompiler compiler = new QueryCompiler();
        return compiler.compile(logicalOperator);
    }
//
//    public EventSchema computeSchema(EPA epa) {
//        return OutputSchemaComputer.computeRecursive(epa, eventStoreProvider);
//    }

    @PreDestroy
    public void close() throws IOException {
        getEventStoreProvider().closeAll();
    }

    public abstract QueryResponse runQueryRequest(QueryRequest query) throws ParseException;

    protected synchronized void clearStream(String streamName) throws IOException {
        EventSchema schema = getSchema(streamName);
        removeStream(streamName);
        registerStream(streamName, schema);
    }

    public ClearStreamRequest createClearStreamRequest(String streamName) {
        if (!getEventStoreProvider().containsKey(streamName)) {
            throw new IllegalArgumentException("Event store " + streamName + " does not exist");
        }
        return clearStreamRequestProvider.create(streamName);
    }

    public boolean confirmClearStreamRequest(ClearStreamRequest clearStreamRequest) throws IOException {
        try {
            String token = clearStreamRequest.getToken();
            ClearStreamRequest foundRequest = clearStreamRequestProvider.get(token);
            String streamName = clearStreamRequest.getStreamName();
            if (foundRequest.getStreamName().equals(streamName)) {
                clearStream(streamName);
                clearStreamRequestProvider.remove(token);
                return true;
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * Manages and provides requests to clear whole streams.
     * To clear whole streams, one must obtain a request token and confirm the request
     * with the token. Those requests are valid for 5 minutes.
     */
    public class ClearStreamRequestProvider {

        PassiveExpiringMap.ConstantTimeToLiveExpirationPolicy<String, ClearStreamRequest>
                expirationPolicy = new PassiveExpiringMap.ConstantTimeToLiveExpirationPolicy<>(
                5, TimeUnit.MINUTES);

        private final PassiveExpiringMap<String, ClearStreamRequest> clearStreamRequests = new PassiveExpiringMap<>(expirationPolicy);

        public ClearStreamRequest create(String streamName) {
            ClearStreamRequest request = ClearStreamRequest.create(streamName);
            put(request);
            return request;
        }

        public void put(ClearStreamRequest clearStreamRequest) {
            clearStreamRequests.put(clearStreamRequest.getToken(), clearStreamRequest);
        }

        public ClearStreamRequest get(String token) {
            return Optional.ofNullable(clearStreamRequests.get(token))
                    .orElseThrow(() -> new IllegalArgumentException("No clear request for given token found.") );
        }

        ClearStreamRequest remove(String token) {
            return Optional.ofNullable(clearStreamRequests.remove(token))
                    .orElseThrow(() -> new IllegalArgumentException("No clear request for given token found.") );
        }
    }

    public class EventStoreProvider implements Iterable<EventStoreImpl> {

        private final Map<String, EventStoreImpl> eventStores = new HashMap<>();

        public void put(EventStoreImpl eventStore) {
            if (eventStores.containsKey(eventStore.getStreamName().toUpperCase())) {
                throw new IllegalArgumentException("Stream \"" + eventStore.getStreamName() + "\" already exists.");
            }
            eventStores.put(eventStore.getStreamName().toUpperCase(), eventStore);
        }

        public EventStoreImpl get(String name) {
            return Optional.ofNullable(eventStores.get(name.toUpperCase()))
                    .orElseThrow(() -> new IllegalArgumentException("Stream \"" + name + "\" does not exist.") );
        }

        public boolean containsKey(String name) {
            return eventStores.containsKey(name.toUpperCase());
        }

        EventStoreImpl remove(String name) {
            return Optional.ofNullable(eventStores.remove(name.toUpperCase()))
                    .orElseThrow(() -> new IllegalArgumentException("Stream \"" + name + "\" does not exist.") );
        }

        public Set<Map.Entry<String, EventStoreImpl>> entrySet() {
            return eventStores.entrySet();
        }

        @Override
        public Iterator<EventStoreImpl> iterator() {
            return eventStores.values().iterator();
        }

        public void closeAll() throws IOException {
            List<Throwable> errors = new ArrayList<>();

            for (EventStoreImpl value : eventStores.values()) {
                try {
                    value.close();
                }
                catch (Throwable se) {
                    errors.add(se);
                }
            }

            if ( !errors.isEmpty() ) {
                IOException t = new IOException("Error while shutting down ChronicleEngine.");
                errors.forEach(t::addSuppressed);
                throw t;
            }
        }
    }

}
