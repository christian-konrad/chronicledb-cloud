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
import java.util.List;


/**
 * Stolen from de.umr.lambda.rest
 */
public class GeoJsonEventDeserializer extends JsonDeserializer<Event> {

	private final int featureIndex;
	private final int attributeCount;
	private final List<KeyValue<String, JsonDeserializer<?>>> attributeDeserializers;
	
	/**
	 * Creates a new EventDeserializer instance
	 */
	public GeoJsonEventDeserializer(EventSchema schema, int featureIndex) {
		this.attributeCount = schema.getNumAttributes();
		this.attributeDeserializers = EventSerializationUtil.getAttributeDeserializers(schema);
		this.featureIndex = featureIndex;
	}
	
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public Event deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		JsonNode node = p.getCodec().readTree(p);
		
		Object[] payload = new Object[attributeCount];
		
		if ( !node.has("geometry") )
			throw new IllegalArgumentException("Geometry is requried for GeoJson features");
		
		payload[featureIndex] = EventSerializationUtil.GEOMETRY_DESERIALIZER.deserialize(node.get("geometry").traverse(p.getCodec()), ctxt);
		
		JsonNode pNode = node.get("properties");
		for ( int i = 0; i < attributeCount; i++ ) {
			if ( i == featureIndex )
				continue;
			KeyValue<String, JsonDeserializer<?>> kv = attributeDeserializers.get(i);
			JsonNode an = pNode.get(kv.getKey());
			if ( an != null && !an.isNull() ) {
				// Next token required for primitives
				JsonParser childParser = an.traverse(p.getCodec());
				childParser.nextToken();
				payload[i] = kv.getValue().deserialize(childParser, ctxt);
			}
		}
		
		if ( !pNode.has(EventSerializationUtil.TSTART) )
			throw new IllegalArgumentException("Events require a timestamp (" + EventSerializationUtil.TSTART + ")");
		
		final long timestamp = (long) (pNode.get(EventSerializationUtil.TSTART).asDouble()*1000.0);
		
		if ( pNode.has(EventSerializationUtil.TEND) )
			return new SimpleEvent(payload, timestamp, (long) (pNode.get(EventSerializationUtil.TEND).asDouble()*1000.0));
		else
			return new SimpleEvent(payload, timestamp);
	}

}
