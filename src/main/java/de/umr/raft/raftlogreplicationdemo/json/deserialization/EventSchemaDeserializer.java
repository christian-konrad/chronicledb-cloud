package de.umr.raft.raftlogreplicationdemo.json.deserialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import de.umr.event.schema.Attribute;
import de.umr.event.schema.EventSchema;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
@JsonComponent
public class EventSchemaDeserializer extends JsonDeserializer<EventSchema> {
	
	private final AttributeDeserializer AS = new AttributeDeserializer();

	/**
	 * @{inheritDoc}
	 */
	@Override
	public EventSchema deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		JsonNode node = p.getCodec().readTree(p);
		List<Attribute> attribs = new ArrayList<>();
		for ( JsonNode an : node ) {
			attribs.add(AS.deserialize(an.traverse(p.getCodec()), ctxt));
		}
		return new EventSchema(attribs);
	}
	
	

}
