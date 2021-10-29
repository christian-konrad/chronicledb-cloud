package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event;

import de.umr.chronicledb.common.query.cursor.Cursor;
import de.umr.chronicledb.common.query.range.Range;
import de.umr.chronicledb.common.util.WrappingIOException;
import de.umr.chronicledb.event.store.AggregatedEventStore;
import de.umr.chronicledb.event.store.EventStore;
import de.umr.chronicledb.event.store.tabPlus.TABPlusEventStoreLoader;
import de.umr.chronicledb.event.store.tabPlus.aggregation.EventAggregationValues;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.EventAggregate;
import de.umr.chronicledb.event.store.tabPlus.aggregation.impl.global.EventCount;
import de.umr.chronicledb.event.store.tabPlus.config.*;
import de.umr.event.Event;
import de.umr.event.schema.Attribute;
import de.umr.event.schema.EventSchema;
import de.umr.jepc.v2.api.epa.EPA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Simple EventStore implementation.
 * Stolen from ChronicleDB Lambda Engine
 */
public class StreamIndex implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(StreamIndex.class);

    public static StreamIndex loadStreamIndex(Path basePath, String streamName) throws IOException {
        log.info("Loading stream index {}", streamName);
        return new StreamIndex(TABPlusEventStoreLoader.load(basePath, streamName));
    }

    public static StreamIndex createStreamIndex(Path basePath, String streamName, EventSchema schema) throws IOException {
        log.info("Creating new stream index {}, schema: {}", streamName, schema);
        return new StreamIndex(TABPlusEventStoreLoader.create( basePath, streamName, getDefaultConfiguration(schema)));
    }

    private final AggregatedEventStore eventStore;

    private StreamIndex(AggregatedEventStore delegate) throws IOException {
        eventStore = delegate;
    }

    public void pushEvent(Event event) {
        try {
            eventStore.insert(event);
        }
        catch (IOException ex) {
            throw new WrappingIOException("Could not insert event: " + event, ex);
        }
    }

    public long eventCount() throws IOException {
        return eventStore.getAggregate(new EventCount(), Long.class).orElse(0L);
    }

    private static TABPlusEventStoreConfigPojo getDefaultConfiguration( EventSchema schema ) {
        List<AttributeConfigPojo> attribs = new ArrayList<>();
        List<LightweightAttributeConfigPojo> attribIndex = new ArrayList<>();

        List<SecondaryIndexConfigPojo> secondaryIndexes = new ArrayList<>();
        BufferConfigPojo bufferPojo = new BufferConfigPojo();
        ContainerConfigPojo secondaryIndexContainers = new ContainerConfigPojo(
                ContainerConfigPojo.ContainerType.BLOCK_FILE, 8192, false, false
        );


        for ( Attribute a : schema ) {
            attribs.add( new AttributeConfigPojo(a.getName(), a.getType(), a.isNullable()) );

            switch ( a.getType() ) {
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    if ( Boolean.parseBoolean(a.getProperty("index")) )
                        attribIndex.add( new LightweightAttributeConfigPojo(a.getName(), List.of("count","max","min","sum")));
                    else if ( Boolean.parseBoolean(a.getProperty("secondaryIndex"))) {
                        secondaryIndexes.add( new SecondaryIndexConfigPojo(a.getName(), SecondaryIndexConfigPojo.Type.BPLUS, secondaryIndexContainers, bufferPojo));
                    }
                    break;
                case STRING:
                    if ( Boolean.parseBoolean(a.getProperty("index")) )
                        attribIndex.add( new LightweightAttributeConfigPojo(a.getName(), List.of("count")));
                    break;
                case GEOMETRY:
                    if ( Boolean.parseBoolean(a.getProperty("index")) )
                        attribIndex.add( new LightweightAttributeConfigPojo(a.getName(), List.of("count","bbox")));
                    else if ( Boolean.parseBoolean(a.getProperty("secondaryIndex"))) {
                        secondaryIndexes.add( new SecondaryIndexConfigPojo(a.getName(), SecondaryIndexConfigPojo.Type.ZBPLUS, secondaryIndexContainers, bufferPojo));
                    }
                    break;
                default:
                    break;

            }
        }

        EventSchemaConfigPojo schemaPojo = new EventSchemaConfigPojo(attribs);

        LightweightConfigPojo lwPojo = new LightweightConfigPojo(List.of("count"), attribIndex);


        ContainerConfigPojo containerPojo = new ContainerConfigPojo(
                ContainerConfigPojo.ContainerType.MACRO_BLOCK,
                8192,
                false,
                false,
                ContainerConfigPojo.CompressionType.LZ4,
                false,
                32768,
                0.1f,
                100,
                50);

        BlockFactoryConfigPojo blockFactoryPojo = new BlockFactoryConfigPojo();

        OutOfOrderConfigPojo oooPojo = new OutOfOrderConfigPojo();

        return new TABPlusEventStoreConfigPojo(
                TABPlusEventStoreConfigPojo.Implementation.CONCURRENT,
                0.1f,
                schemaPojo,
                lwPojo,
                bufferPojo,
                containerPojo,
                blockFactoryPojo,
                oooPojo,
                secondaryIndexes
        );
    }

    @Override
    public Cursor<Event> filter(Range<Long> keyRange, Predicate<Event> predicate) throws IllegalStateException, IOException {
        return eventStore.filter(keyRange, predicate);
    }

    public Cursor<Event> filter(Predicate<Event> predicate, Predicate<EventAggregationValues> smaPredicate) throws IllegalStateException, IOException {
        return eventStore.filter(predicate, smaPredicate);
    }

    public Cursor<Event> filter(Range<Long> range, Predicate<Event> predicate, Predicate<EventAggregationValues> predicate1) throws IllegalStateException, IOException {
        return eventStore.filter(range, predicate, predicate1);
    }

    public Optional<?> getAggregate(EventAggregate aggregate) throws IllegalStateException, IOException {
        return eventStore.getAggregate(aggregate);
    }

    public Optional<?> getAggregate(Range<Long> keyRange, EventAggregate aggregate) throws IllegalStateException, IOException {
        return eventStore.getAggregate(keyRange, aggregate);
    }

    public <T> Optional<T> getAggregate(EventAggregate aggregate, Class<T> type) throws IOException {
        return eventStore.getAggregate(aggregate, type);
    }

    public <T> Optional<T> getAggregate(EventAggregate aggregate, Range<Long> keyRange, Class<T> type) throws IOException {
        return eventStore.getAggregate(aggregate, keyRange, type);
    }

    public EventAggregationValues getAggregates(List<? extends EventAggregate> aggregates) throws IllegalStateException, IOException {
        return eventStore.getAggregates(aggregates);
    }

    public EventAggregationValues getAggregates(Range<Long> range, List<? extends EventAggregate> list) throws IllegalStateException, IOException {
        return eventStore.getAggregates(range, list);
    }

    @Override
    public String getStreamName() {
        return eventStore.getStreamName();
    }

    @Override
    public EventSchema getSchema() {
        return eventStore.getSchema();
    }

    @Override
    public EPA optimizeEPA(EPA query) throws IOException {
        return eventStore.optimizeEPA(query);
    }

    @Override
    public EPA optimizeEPA(EPA query, Range<Long> timeInterval) throws IOException {
        return eventStore.optimizeEPA(query, timeInterval);
    }

    @Override
    public Optional<Range<Long>> getKeyRange() {
        return eventStore.getKeyRange();
    }

    @Override
    public Cursor<Event> scan(Range<Long> range) throws IllegalStateException, IOException {
        return eventStore.scan(range);
    }

    @Override
    public Cursor<Event> filter(Predicate<Event> predicate) throws IllegalStateException, IOException {
        return eventStore.filter(predicate);
    }

    @Override
    public Cursor<Event> get(Long aLong) throws IllegalStateException, IOException {
        return eventStore.get(aLong);
    }

    @Override
    public void insert(Event event) throws IllegalStateException, IOException {
        eventStore.insert(event);
    }

    @Override
    public void insert(Iterator<Event> values, boolean ordered) throws IllegalStateException, IOException {
        eventStore.insert(values, ordered);
    }

    @Override
    public Cursor<Event> scan() throws IllegalStateException, IOException {
        return eventStore.scan();
    }

    @Override
    public Optional<Event> getFirst(Long aLong) throws IllegalStateException, IOException {
        return eventStore.getFirst(aLong);
    }

    @Override
    public void close() throws IOException {
        eventStore.close();
    }

    @Override
    public boolean isOpen() {
        return eventStore.isOpen();
    }
}
