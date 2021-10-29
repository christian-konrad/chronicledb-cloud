package de.umr.raft.raftlogreplicationdemo.json.serialization.event;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import de.umr.chronicledb.common.util.KeyValue;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.json.serialization.util.EventSerializationUtil;

import java.io.IOException;
import java.util.List;


/**
 * 
 */
public class EventSerializer extends JsonSerializer<Event> {
	
	private final int attributeCount;
	private final List<KeyValue<String, JsonSerializer<Object>>> attributeSerializers;
	
	/**
	 * Creates a new EventSerializer instance
	 */
	public EventSerializer(EventSchema schema) {
		this.attributeCount = schema.getNumAttributes();
		this.attributeSerializers = EventSerializationUtil.getAttributeSerializers(schema);
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void serialize(Event value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartObject();
        for (int i = 0; i < attributeCount; i++) {
        	KeyValue<String,JsonSerializer<Object>> kv = attributeSerializers.get(i);
        	Object av = value.get(i);
            gen.writeFieldName(kv.getKey());
            if ( av == null )
            	gen.writeNull();
            else
            	kv.getValue().serialize(av, gen, serializers);
        }
        gen.writeNumberField(EventSerializationUtil.TSTART, value.getT1());
        gen.writeNumberField(EventSerializationUtil.TEND, value.getT2());
        gen.writeEndObject();
	}
}
