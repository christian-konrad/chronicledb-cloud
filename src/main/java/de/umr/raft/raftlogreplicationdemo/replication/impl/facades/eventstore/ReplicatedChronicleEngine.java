package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.event.schema.SchemaProvider;
import de.umr.jepc.v2.api.epa.EPA;
import de.umr.lambda.jepc.ql.QueryCompiler;
import de.umr.lambda.ql.lang.EPQueryCompiler;
import de.umr.lambda.ql.operators.LogicalOperator;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryRequest;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryResponse;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.EventStoreReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.EventStoreStateMachine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamInfo;
import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.SystemInfoService;
import de.umr.raft.raftlogreplicationdemo.util.RaftGroupUtil;
import lombok.SneakyThrows;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Component
public class ReplicatedChronicleEngine implements SchemaProvider, AutoCloseable {

    Logger LOG = LoggerFactory.getLogger(ReplicatedChronicleEngine.class);

    private final EventStoreProvider eventStoreProvider;

    // TODO actual ClusterManagementReplicationClient to spawn new streams
    private final ClusterMetadataReplicationClient metadataClient;
    private final SystemInfoService systemInfoService;
    private final RaftConfig raftConfig;

    private boolean eventStoreProviderInitialized = false;

    @Autowired
    public ReplicatedChronicleEngine(ClusterMetadataReplicationClient metadataClient, SystemInfoService systemInfoService, RaftConfig raftConfig) throws IOException, ExecutionException, InterruptedException {
        LOG.info("Starting ReplicatedChronicleEngine");

        this.metadataClient = metadataClient;
        this.systemInfoService = systemInfoService;
        this.raftConfig = raftConfig;

        eventStoreProvider = new EventStoreProvider();

        LOG.info("Started ReplicatedChronicleEngine");
    }

    // TODO can only obtain them later once server started. Same problem as with partition creation:
    // Need separate raft management server
    @SneakyThrows
    private void initEventStoreProvider() {
        if (eventStoreProviderInitialized) return;
        List<String> streamNames = getStreamNames();
        for (String name : streamNames) {
            val client = new EventStoreReplicationClient(raftConfig, name);
            eventStoreProvider.put(BufferedReplicatedEventStore.of(name, client,
                    raftConfig.getEventStoreBufferSize(), raftConfig.getEventStoreBufferTimeout()));
        }
        eventStoreProviderInitialized = true;
    }

    private EventStoreProvider getEventStoreProvider() {
        if (!eventStoreProviderInitialized) initEventStoreProvider();
        return eventStoreProvider;
    }

    public List<String> getStreamNames() throws IOException, ExecutionException, InterruptedException {
        // TODO this is something that must be done by clusterManagementStateMachine via client

        val raftGroups = systemInfoService.getRaftGroups();
        val eventStoreRaftGroups = RaftGroupUtil.filterRaftGroupsByStateMachine(raftGroups, EventStoreStateMachine.class);
        val streamNames = RaftGroupUtil.getPartitionNamesFromRaftGroupInfos(eventStoreRaftGroups);
        return streamNames;
    }

    public StreamInfo getStreamInfo(String name) throws IOException {
        BufferedReplicatedEventStore eventStore = getEventStoreProvider().get(name);
        return new StreamInfo(name, eventStore.eventCount(), eventStore.getKeyRange().orElse(null), eventStore.getSchema());
    }

    public Optional<?> getAggregate(String streamName, Range<Long> keyRange, EventAggregate aggregate)
            throws IllegalStateException, IOException {
        BufferedReplicatedEventStore eventStore = getEventStoreProvider().get(streamName);
        return eventStore.getAggregate(keyRange, aggregate);
    }

    public Optional<?> getAggregate(String streamName, EventAggregate aggregate)
            throws IllegalStateException, IOException {
        BufferedReplicatedEventStore eventStore = getEventStoreProvider().get(streamName);
        return eventStore.getAggregate(aggregate);
    }

    public synchronized EventStoreProvider registerStream(String name, EventSchema schema) throws IOException {
        // TODO allow eventSchemas by protobuf
        // TODO register a new raft group using managementClient

        throw new UnsupportedOperationException("Currently not implemented");

        /* original code of ChronicleEngine for reference
        final String normalizedName = name.toUpperCase();
        final StreamIndex index = StreamIndex.createStreamIndex(path, normalizedName, schema);
        indexes.put(index);
        Files.write(basePath.resolve(META_FILE_NAME), Collections.singletonList(normalizedName),
                StandardOpenOption.APPEND);
        return index;
        */
    }

    public void pushEvent(String stream, Event event) throws IOException, InterruptedException {
        val eventStore = getEventStoreProvider().get(stream);

        if (raftConfig.isEventStoreBufferEnabled()) {
            eventStore.insertBuffered(event);
        } else {
            eventStore.insert(event);
        }
    }

    public synchronized void removeStream(String name) throws IOException {
        throw new UnsupportedOperationException("Currently not implemented");

        // final String normalizedName = name.toUpperCase();
        // eventStoreProvider.remove(normalizedName).close();

        // TODO handle this using managementGroup; deleting the raft group for the stream (check if it deletes all files, too)
    }

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

    public QueryResponse runQueryRequest(QueryRequest query) {
        // TODO (PREFERED) just send queryRequest to state machines/all raft groups and then apply all upcoming stuff there
        // TODO how to run distributed querys BETWEEN raft groups to enable JOINs??
        //String queryLabel = "Query" + counter.getAndIncrement();
        /*
        String queryLabel = "Query"; // whats that for?
        EPA translate = chronicleEngine.translateQuery(query.getQueryString());
        EventSchema eventSchema = chronicleEngine.computeSchema(translate);
        QueryDescription queryDescription = new QueryDescription(queryLabel, translate, new Range<>(query.getStartTime(), query.getEndTime(), true, true));
        */
        return null;
        // TODO OR may send query description to state machine; requestHandler must be implemented by state machine
        // TODO to do so, check if there is an easy way to serialize the EPA translate
        // TODO must adjust request handler to know which stream to send this...
        //Cursor<? extends Event> eventCursor = requestHandler.postRequest(queryDescription).get();
        //return new QueryResponse(eventCursor, eventSchema);
    }

    public static class EventStoreProvider implements Iterable<BufferedReplicatedEventStore> {

        private final Map<String, BufferedReplicatedEventStore> eventStores = new HashMap<>();

        public void put(BufferedReplicatedEventStore eventStore) {
            if (eventStores.containsKey(eventStore.getStreamName().toUpperCase())) {
                throw new IllegalArgumentException("Stream \"" + eventStore.getStreamName() + "\" already exists.");
            }
            eventStores.put(eventStore.getStreamName().toUpperCase(), eventStore);
        }

        public BufferedReplicatedEventStore get(String name) {
            return Optional.ofNullable(eventStores.get(name.toUpperCase()))
                    .orElseThrow(() -> new IllegalArgumentException("Stream \"" + name + "\" does not exist.") );
        }

        public boolean containsKey(String name) {
            return eventStores.containsKey(name.toUpperCase());
        }

        private BufferedReplicatedEventStore remove(String name) {
            return Optional.ofNullable(eventStores.remove(name.toUpperCase()))
                    .orElseThrow(() -> new IllegalArgumentException("Stream \"" + name + "\" does not exist.") );
        }

        public Set<Map.Entry<String, BufferedReplicatedEventStore>> entrySet() {
            return eventStores.entrySet();
        }

        @Override
        public Iterator<BufferedReplicatedEventStore> iterator() {
            return eventStores.values().iterator();
        }

        public void closeAll() throws IOException {
            List<Throwable> errors = new ArrayList<>();

            for (BufferedReplicatedEventStore value : eventStores.values()) {
                try {
                    value.close();
                }
                catch (Throwable se) {
                    errors.add(se);
                }
            }

            if ( !errors.isEmpty() ) {
                IOException t = new IOException("Error while shutting down ReplicatedChronicleEngine.");
                errors.forEach(t::addSuppressed);
                throw t;
            }
        }
    }

}
