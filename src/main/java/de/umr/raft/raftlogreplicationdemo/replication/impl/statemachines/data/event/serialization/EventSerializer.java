package de.umr.raft.raftlogreplicationdemo.replication.impl.statemachines.data.event.serialization;

import de.umr.chronicledb.common.serialization.primitives.GeometrySerializer;
import de.umr.chronicledb.common.serialization.primitives.StringSerializer;
import de.umr.event.Event;
import de.umr.event.schema.Attribute;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventProto;
import de.umr.raft.raftlogreplicationdemo.replication.api.proto.EventSerializationType;
import lombok.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.locationtech.jts.geom.Geometry;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

// TODO should cache the serializer per eventSchema and serializationType
@RequiredArgsConstructor(staticName = "of")
public class EventSerializer {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    @Getter @NonNull private final EventSchema eventSchema;
    @Getter @Setter private EventSerializationType serializationType = EventSerializationType.NATIVE;

    private Event fromProtoUsingNativeSerializer(EventProto eventProto) {
        val serializedEvent = eventProto.getNativeSerializedEvent();
        val serializer = de.umr.chronicledb.event.serialization.EventSerializer.create(eventSchema);
        return serializer.deserialize(serializedEvent.asReadOnlyByteBuffer());
    }

    public Event fromProto(EventProto eventProto) {
        if (eventProto.getSerializationType().equals(EventSerializationType.NATIVE)) {
            return fromProtoUsingNativeSerializer(eventProto);
        }

        throw new UnsupportedOperationException("Currently only native (Java ChronicleDB) deserialization is supported");

//        // TODO later use protobuf to ensure platform independency and make advantage of variable byte length encoding;
//        // compare with chronicle built-in
//
//        val t1 = eventProto.getT1();
//        val t2 = eventProto.getT2();
//
//        val payload = eventProto.getPayloadList();
//        return new SimpleEvent(payload, t1, t2);
    }

    public List<Event> fromProto(List<EventProto> eventProtos) {
        return eventProtos.stream().map(this::fromProto).collect(Collectors.toList());
    }

    /**
     * Estimates the size in bytes required to serialize the given event attribute value.
     *
     * @param type The data type.
     * @return The estimated size in bytes required to serialize the given data type.
     */
    private static int estimateEventAttributeSize(Event event, int attributeIndex, Attribute.DataType type) {
        switch (type) {
            case BOOLEAN:
            case BYTE:
                return Byte.BYTES;
            case SHORT:
                return Short.BYTES;
            case INTEGER:
                return Integer.BYTES;
            case LONG:
                return Long.BYTES;
            case FLOAT:
                return Float.BYTES;
            case DOUBLE:
                return Double.BYTES;
            case STRING:
                return event.get(attributeIndex, String.class).getBytes(DEFAULT_CHARSET).length
                        + Short.BYTES; // The length field (?)
            case GEOMETRY:
                return new GeometrySerializer().getSerializedSize(event.get(attributeIndex, Geometry.class));
            default:
                throw new IllegalArgumentException(
                        String.format("Serialization of %s-attributes not supported.", type));
        }
    }

    // unwrapping the event before serialization seems to be obsolete and inefficient...
    /**
     * Gets the size in bytes required to serialize the current event by the given schema.
     *
     * @return The size in bytes required to serialize the current event.
     */
    public int getEstimatedEventSize(Event event) {
        var tmpSize = Long.BYTES; // TODO why initial byte?
        var nullAttribs = 0;

        for (Attribute attr : eventSchema) {
            val index = eventSchema.getAttributeIndex(attr.getName());
            val type = attr.getType();
            tmpSize += estimateEventAttributeSize(event, index, type);
            if (attr.isNullable()) nullAttribs++;
        }
        // TODO is this step really needed?
        val nullmapBytes = (nullAttribs + Byte.SIZE - 1) / Byte.SIZE;
        return tmpSize + nullmapBytes;
    }

    private ByteBuffer serializeEventToByteBuffer(Event event, de.umr.chronicledb.event.serialization.EventSerializer serializer, int currentAttemptedBufferSize, int currentAttempt) {
        // TODO print to log of there is more than one attempt
        if (currentAttempt > 4) {
            // should never need to double buffer size 4 times...
            throw new BufferOverflowException();
        }
        val byteBuffer = ByteBuffer.allocate(currentAttemptedBufferSize);
        try {
            serializer.serialize(event, byteBuffer);
            return byteBuffer;
        } catch (BufferOverflowException e) {
            // double buffer size, retry
            return serializeEventToByteBuffer(event, serializer, currentAttemptedBufferSize * 2, currentAttempt + 1);
        }
    }

    private EventProto toProtoUsingNativeSerializer(Event event) throws BufferOverflowException {
        val serializer = de.umr.chronicledb.event.serialization.EventSerializer.create(eventSchema);
        val byteBuffer = serializeEventToByteBuffer(event, serializer, getEstimatedEventSize(event), 1);

        byteBuffer.rewind();
        val byteString = ByteString.copyFrom(byteBuffer);

        return EventProto.newBuilder()
                .setSerializationType(EventSerializationType.NATIVE)
                .setNativeSerializedEvent(byteString)
                .build();
    }

    public EventProto toProto(Event event) throws InvalidProtocolBufferException {
        if (serializationType.equals(EventSerializationType.NATIVE)) {
            return toProtoUsingNativeSerializer(event);
        }

        throw new UnsupportedOperationException("Currently only native (Java ChronicleDB) serialization is supported");

        // TODO build proto serialization as allocating byte buffers is kind of a nightmare
    }

    public Iterator<EventProto> toProto(Iterator<Event> events) {
        return new Iterator<EventProto>() {
            @Override
            public boolean hasNext() {
                return events.hasNext();
            }

            @Override
            public EventProto next() {
                try {
                    return toProto(events.next());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        };
    }
}
