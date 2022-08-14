package de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore;

import de.umr.chronicledb.io.util.IOUtil;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.config.RaftConfig;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryRequest;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.QueryResponse;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamIndex;
import de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.StreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class EmbeddedChronicleEngine extends BaseChronicleEngine<StreamIndex> {

    Logger LOG = LoggerFactory.getLogger(EmbeddedChronicleEngine.class);

    private Path basePath;
    private final RaftConfig raftConfig;
    private static final String	META_FILE_NAME= "engine.mf";
    //private final RequestHandler<QueryDescription, Cursor<Event>> requestHandler;

    // RequestHandler<QueryDescription, Cursor<Event>> requestHandler
    @Autowired
    public EmbeddedChronicleEngine(RaftConfig raftConfig) {
        super();

        LOG.info("Starting EmbeddedChronicleEngine");

        this.raftConfig = raftConfig;
        this.basePath = Path.of(raftConfig.getStoragePath(), "chronicledb", "embedded");
        //this.requestHandler = requestHandler; // TODO rename to QueryHandler ?

        initEventStoreProvider();

        LOG.info("Started EmbeddedChronicleEngine");
    }

    @Override
    protected void initStreams() throws IOException {
        final Path metaFile = basePath.resolve(META_FILE_NAME);
        if (!Files.exists(basePath)) {
            LOG.debug("Storage-Directory does not exist. Creating.");
            Files.createDirectories(basePath);
        }

        if (Files.exists(metaFile)) {
            List<String> streamNames = Files.readAllLines(metaFile);
            for (String name : streamNames) {
                Path path = basePath.resolve(name);
                eventStoreProvider.put(StreamIndex.loadStreamIndex(path, name));
            }
        } else {
            Files.createFile(metaFile);
        }
    }

    public StreamInfo getStreamInfo(String name) throws IOException {
        StreamIndex si = getEventStoreProvider().get(name);
        return new StreamInfo(name, si.eventCount(), si.getKeyRange().orElse(null), si.getSchema());
    }

    public synchronized StreamIndex registerStream(String name, EventSchema schema) throws IOException {
        final String normalizedName = name.toUpperCase();
        final Path path = basePath.resolve(normalizedName);
        final StreamIndex index = StreamIndex.createStreamIndex(path, normalizedName, schema);
        eventStoreProvider.put(index);
        Files.write(basePath.resolve(META_FILE_NAME), Collections.singletonList(normalizedName),
                StandardOpenOption.APPEND);
        return index;
    }

    @Override
    public void pushEvent(String stream, Event event) {
        eventStoreProvider.get(stream).pushEvent(event);
    }

    @Override
    public synchronized void removeStream(String name) throws IOException {
        final String normalizedName = name.toUpperCase();

        final Path metaPath = basePath.resolve(META_FILE_NAME);
        final Path metaTmpPath = basePath.resolve(META_FILE_NAME + ".tmp");
        final Path streamDir = basePath.resolve(normalizedName);

        List<String> stripped = Files.lines(metaPath).filter(x -> !x.equals(normalizedName)).collect(Collectors.toList());
        Files.deleteIfExists(metaTmpPath);
        Files.write(metaTmpPath, stripped, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        Files.deleteIfExists(metaPath);
        Files.move(metaTmpPath, metaPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        IOUtil.deleteDirectoryRecursively(streamDir);

        eventStoreProvider.remove(normalizedName).close();
    }

    @Override
    public QueryResponse runQueryRequest(QueryRequest query) throws ParseException {
        // TODO (PREFERED) just send queryRequest to state machines/all raft groups and then apply all upcoming stuff there
        // TODO how to run distributed querys BETWEEN raft groups to enable JOINs??
        //String queryLabel = "Query" + counter.getAndIncrement();

//        String queryLabel = "Query"; // whats that for?
//        EPA translate = translateQuery(query.getQueryString());
//        EventSchema eventSchema = computeSchema(translate);
//        QueryDescription queryDescription = new QueryDescription(queryLabel, translate, new Range<>(query.getStartTime(), query.getEndTime(), true, true));
//
//
//        Cursor<? extends Event> eventCursor = requestHandler.postRequest(queryDescription).get();
//        return new QueryResponse(eventCursor, eventSchema);

        return null;
    }

//    public EventSchema computeSchema(EPA epa) {
//        return OutputSchemaComputer.computeRecursive(epa, indexes);
//    }


}
