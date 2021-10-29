package de.umr.raft.raftlogreplicationdemo.json.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import de.umr.event.schema.Attribute;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.json.serialization.primitives.AttributeSerializer;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;

/**
 * 
 */
@JsonComponent
public class EventSchemaSerializer extends JsonSerializer<EventSchema> {
	
	private final AttributeSerializer AS = new AttributeSerializer();

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void serialize(EventSchema value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartArray();
		for ( Attribute a : value )
			AS.serialize(a, gen, serializers);
		gen.writeEndArray();
	}
	
	

}
