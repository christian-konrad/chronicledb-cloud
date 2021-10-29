package de.umr.raft.raftlogreplicationdemo.json.deserialization.event;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import de.umr.chronicledb.common.util.KeyValue;
import de.umr.event.Event;
import de.umr.event.impl.SimpleEvent;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.json.serialization.util.EventSerializationUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Stolen from de.umr.lambda.rest
 */
public class EventDeserializer extends JsonDeserializer<Event> {

	private final int attributeCount;
	private final List<KeyValue<String, JsonDeserializer<?>>> attributeDeserializers;
	
	/**
	 * Creates a new EventDeserializer instance
	 */
	public EventDeserializer(EventSchema schema) {
		this.attributeCount = schema.getNumAttributes();
		this.attributeDeserializers = EventSerializationUtil.getAttributeDeserializers(schema);
	}
	
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public Event deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		JsonNode node = p.getCodec().readTree(p);
		
		Map<String, String> fieldMapping = new HashMap<>();
		
		node.fieldNames().forEachRemaining( f -> fieldMapping.put(f.toUpperCase(), f));
		
		Object[] payload = new Object[attributeCount];
		for ( int i = 0; i < attributeCount; i++ ) {
			KeyValue<String, JsonDeserializer<?>> kv = attributeDeserializers.get(i);
			JsonNode an = node.get(fieldMapping.get(kv.getKey()));
			if ( an != null && !an.isNull() ) {
				// Next token required for primitives
				JsonParser childParser = an.traverse(p.getCodec());
				childParser.nextToken();
				payload[i] = kv.getValue().deserialize(childParser, ctxt);
			}
		}
		
		if ( !fieldMapping.containsKey(EventSerializationUtil.TSTART.toUpperCase()) )
			throw new IllegalArgumentException("Events require a timestamp (" + EventSerializationUtil.TSTART + ")");
		
		final long timestamp = node.get(fieldMapping.get(EventSerializationUtil.TSTART.toUpperCase())).asLong();
		
		if ( fieldMapping.containsKey(EventSerializationUtil.TEND.toUpperCase()) )
			return new SimpleEvent(payload, timestamp, node.get(fieldMapping.get(EventSerializationUtil.TEND.toUpperCase())).asLong());
		else
			return new SimpleEvent(payload, timestamp);
	}

}
