package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.jepc.v2.api.epa.EPA;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryRequest;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryResponse;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.EventStoreReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.EventStoreStateMachine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamInfo;
import de.umr.raft.raftlogreplicationdemo.services.impl.sysinfo.SystemInfoService;
import de.umr.raft.raftlogreplicationdemo.util.RaftGroupUtil;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class ReplicatedChronicleEngine extends BaseChronicleEngine<BufferedReplicatedEventStore> {

    // TODO also consider reader/writer facades, or pub/sub facades...

    Logger LOG = LoggerFactory.getLogger(ReplicatedChronicleEngine.class);

    // TODO actual ClusterManagementReplicationClient to spawn new streams
    private final ClusterMetadataReplicationClient metadataClient;
    private final SystemInfoService systemInfoService;
    private final RaftConfig raftConfig;

    @Autowired
    public ReplicatedChronicleEngine(ClusterMetadataReplicationClient metadataClient, SystemInfoService systemInfoService, RaftConfig raftConfig) throws IOException, ExecutionException, InterruptedException {
        super();

        LOG.info("Starting ReplicatedChronicleEngine");

        this.metadataClient = metadataClient;
        this.systemInfoService = systemInfoService;
        this.raftConfig = raftConfig;

        LOG.info("Started ReplicatedChronicleEngine");
    }

    // TODO can only obtain them later once server started. Same problem as with partition creation:
    // Need separate raft management server
    @Override
    protected void initStreams() {
        List<String> streamNames = null;
        try {
            streamNames = getStreamNames();
            for (String name : streamNames) {
                val client = new EventStoreReplicationClient(raftConfig, name);
                eventStoreProvider.put(BufferedReplicatedEventStore.of(name, client,
                        raftConfig.getEventStoreBufferSize(), raftConfig.getEventStoreBufferTimeout()));
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<String> getStreamNames() throws IOException, ExecutionException, InterruptedException {
        // TODO this is something that must be done by clusterManagementStateMachine via client

        val raftGroups = systemInfoService.getRaftGroups();
        val eventStoreRaftGroups = RaftGroupUtil.filterRaftGroupsByStateMachine(raftGroups, EventStoreStateMachine.class);
        val streamNames = RaftGroupUtil.getPartitionNamesFromRaftGroupInfos(eventStoreRaftGroups);
        return streamNames;
    }

    @Override
    public StreamInfo getStreamInfo(String name) throws IOException {
        BufferedReplicatedEventStore eventStore = getEventStoreProvider().get(name);
        return new StreamInfo(name, eventStore.eventCount(), eventStore.getKeyRange().orElse(null), eventStore.getSchema());
    }

    @Override
    public synchronized BufferedReplicatedEventStore registerStream(String name, EventSchema schema) throws IOException {
        // TODO allow eventSchemas by protobuf
        // TODO register a new raft group using managementClient

        throw new UnsupportedOperationException("Currently not implemented");
    }

    @Override
    public void pushEvent(String stream, Event event) throws IOException, InterruptedException {
        val eventStore = getEventStoreProvider().get(stream);

        if (raftConfig.isEventStoreBufferEnabled()) {
            eventStore.insertBuffered(event);
        } else {
            eventStore.insert(event);
        }
    }

    @Override
    public synchronized void removeStream(String name) {
        throw new UnsupportedOperationException("Currently not implemented");

        // final String normalizedName = name.toUpperCase();
        // eventStoreProvider.remove(normalizedName).close();

        // TODO handle this using managementGroup; deleting the raft group for the stream (check if it deletes all files, too)
    }

    @Override
    public QueryResponse runQueryRequest(QueryRequest query) {
        // TODO (PREFERED) just send queryRequest to state machines/all raft groups and then apply all upcoming stuff there
        // TODO how to run distributed querys BETWEEN raft groups to enable JOINs??
        //String queryLabel = "Query" + counter.getAndIncrement();

//        String queryLabel = "Query"; // whats that for?
//        EPA translate = translateQuery(query.getQueryString());
//        EventSchema eventSchema = computeSchema(translate);
//        QueryDescription queryDescription = new QueryDescription(queryLabel, translate, new Range<>(query.getStartTime(), query.getEndTime(), true, true));

        return null;
        // TODO OR may send query description to state machine; requestHandler must be implemented by state machine
        // TODO to do so, check if there is an easy way to serialize the EPA translate
        // TODO must adjust request handler to know which stream to send this...
//        Cursor<? extends Event> eventCursor = requestHandler.postRequest(queryDescription).get();
        //return new QueryResponse(eventCursor, eventSchema);
    }

    protected synchronized void clearStream(String streamName) throws IOException {
        val eventStore = getEventStoreProvider().get(streamName);
        eventStore.clear();
    }

}
