package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.jepc.v2.api.epa.EPA;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryRequest;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryResponse;
import de.umr.raft.raftlogreplicationdemo.replication.api.PartitionInfo;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.ClusterMetadataReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.clients.EventStoreReplicationClient;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.clustermanagement.ClusterManager;
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
import java.util.stream.Collectors;

// TODO start again once cluster management works

@Component
public class ReplicatedChronicleEngine extends BaseChronicleEngine<BufferedReplicatedEventStore> {

    // TODO also consider reader/writer facades, or pub/sub facades...

    Logger LOG = LoggerFactory.getLogger(ReplicatedChronicleEngine.class);

    // TODO actual ClusterManagementReplicationClient to spawn new streams
    private final ClusterMetadataReplicationClient metadataClient;
    //private final SystemInfoService systemInfoService;
    private final ClusterManager clusterManager;
    private final RaftConfig raftConfig;

    @Autowired
    public ReplicatedChronicleEngine(ClusterMetadataReplicationClient metadataClient, SystemInfoService systemInfoService, ClusterManager clusterManager, RaftConfig raftConfig) throws IOException, ExecutionException, InterruptedException {
        super();

        LOG.info("Starting ReplicatedChronicleEngine");

        this.metadataClient = metadataClient;
        //this.systemInfoService = systemInfoService;
        this.clusterManager = clusterManager;
        this.raftConfig = raftConfig;

        LOG.info("Started ReplicatedChronicleEngine");
    }

    private void addKnownStream(PartitionInfo partition) {
        var partitionName = partition.getPartitionName().getName();
        val client = new EventStoreReplicationClient(
                raftConfig,
                partitionName,
                partition.getRaftGroup());
        eventStoreProvider.put(BufferedReplicatedEventStore.of(partitionName, client,
                raftConfig.getEventStoreBufferSize(), raftConfig.getEventStoreBufferTimeout()));
    }

    @Override
    protected void initStreams() {
        List<PartitionInfo> partitions = null;
        List<String> streamNames = null;
        try {
            partitions = getPartitions();
            //streamNames = getStreamNames();
            //for (String name : streamNames) {
            for (PartitionInfo partition : partitions) {
                addKnownStream(partition);
//                var partitionName = partition.getPartitionName().getName();
//                val client = new EventStoreReplicationClient(
//                        raftConfig,
//                        partitionName,
//                        partition.getRaftGroup());
//                eventStoreProvider.put(BufferedReplicatedEventStore.of(partitionName, client,
//                        raftConfig.getEventStoreBufferSize(), raftConfig.getEventStoreBufferTimeout()));
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<String> getStreamNames() throws IOException, ExecutionException, InterruptedException {
        LOG.info("Requesting stream names");
        //val raftGroups = clusterManager.listPartitions(EventStoreStateMachine.class).stream().map(PartitionInfo::getRaftGroup).collect(Collectors.toList());
        val partitionNames = clusterManager.listPartitions(EventStoreStateMachine.class).stream().map(PartitionInfo::getPartitionName).collect(Collectors.toList());
        return partitionNames.stream().map(partitionName -> partitionName.getName()).collect(Collectors.toList());

        //val eventStoreRaftGroups = RaftGroupUtil.filterRaftGroupsByStateMachine(raftGroups, EventStoreStateMachine.class);
        //val streamNames = RaftGroupUtil.getPartitionNamesFromRaftGroupInfos(eventStoreRaftGroups);
        //return streamNames;
    }

    public List<PartitionInfo> getPartitions() throws IOException, ExecutionException, InterruptedException {
        LOG.info("Requesting partitions");
        //val raftGroups = clusterManager.listPartitions(EventStoreStateMachine.class).stream().map(PartitionInfo::getRaftGroup).collect(Collectors.toList());
        val partitions = clusterManager.listPartitions(EventStoreStateMachine.class);
        return partitions;

        //val eventStoreRaftGroups = RaftGroupUtil.filterRaftGroupsByStateMachine(raftGroups, EventStoreStateMachine.class);
        //val streamNames = RaftGroupUtil.getPartitionNamesFromRaftGroupInfos(eventStoreRaftGroups);
        //return streamNames;
    }

    @Override
    public StreamInfo getStreamInfo(String name) throws IOException {
        BufferedReplicatedEventStore eventStore = getEventStoreProvider().get(name);
        return new StreamInfo(name, eventStore.eventCount(), eventStore.getKeyRange().orElse(null), eventStore.getSchema());
    }

    @Override
    public synchronized BufferedReplicatedEventStore registerStream(String name, EventSchema schema) throws IOException {
        // TODO allow eventSchemas by protobuf

        //throw new UnsupportedOperationException("Currently not implemented");
        var partitionInfo = clusterManager.registerPartition(
                EventStoreStateMachine.class,
                name,
                3);

        // add to known streams

        return null;
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
