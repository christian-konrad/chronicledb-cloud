package de.umr.raft.raftlogreplicationdemo.json.deserialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import de.umr.event.schema.Attribute;
import de.umr.event.schema.Attribute.DataType;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;


/**
 * 
 */
@JsonComponent
public class AttributeDeserializer extends JsonDeserializer<Attribute> {

	/**
	 * @{inheritDoc}
	 */
	@Override
	public Attribute deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		JsonNode node = p.getCodec().readTree(p);
		Attribute attrib = new Attribute(node.get("name").asText(), DataType.valueOf(node.get("type").asText().toUpperCase()));
		if ( node.has("properties") )
			node.get("properties").fields().forEachRemaining( e -> attrib.setProperty(e.getKey(), e.getValue().asText()));
		return attrib;
	}

}
