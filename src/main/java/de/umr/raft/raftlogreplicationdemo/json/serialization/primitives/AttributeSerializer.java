package de.umr.raft.raftlogreplicationdemo.json.serialization.primitives;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import de.umr.event.schema.Attribute;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
@JsonComponent
public class AttributeSerializer extends JsonSerializer<Attribute> {
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public void serialize(Attribute value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		Map<String, String> properties = new HashMap<String, String>();
		value.getProperties().forEach( key -> properties.put(key, value.getProperty(key)));
		gen.writeStartObject();
		gen.writeStringField("name", value.getName());
		gen.writeStringField("type", value.getType().name());
		gen.writeObjectField("properties", properties);
		gen.writeEndObject();
		
	}

}
