package de.umr.raft.raftlogreplicationdemo.json.serialization.event;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import de.umr.chronicledb.common.util.KeyValue;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.json.serialization.util.EventSerializationUtil;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.util.List;

/**
 * 
 */
public class GeoJsonEventSerializer extends JsonSerializer<Event> {

	private final int												attributeCount;
	private final List<KeyValue<String, JsonSerializer<Object>>>	attributeSerializers;
	private final int												featureIndex;

	/**
	 * Creates a new GeoJsonEventSerializer instance
	 * 
	 * @param schema
	 */
	public GeoJsonEventSerializer(EventSchema schema, int featureIndex) {
		this.attributeCount = schema.getNumAttributes();
		this.attributeSerializers = EventSerializationUtil.getAttributeSerializers(schema);
		this.featureIndex = featureIndex;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void serialize(Event value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartObject();
		gen.writeStringField("type", "Feature");
		gen.writeFieldName("geometry");
		EventSerializationUtil.GEOMETRY_SERIALIZER.serialize(value.get(featureIndex, Geometry.class), gen, serializers);

		gen.writeObjectFieldStart("properties");
		for (int i = 0; i < attributeCount; i++) {
			if ( i == featureIndex )
				continue;
			KeyValue<String, JsonSerializer<Object>> kv = attributeSerializers.get(i);
			Object av = value.get(i);
			gen.writeFieldName(kv.getKey());
			if ( av == null )
				gen.writeNull();
			else
				kv.getValue().serialize(av, gen, serializers);
		}
		gen.writeNumberField(EventSerializationUtil.TSTART, value.getT1() / 1000d);
		gen.writeNumberField(EventSerializationUtil.TEND, value.getT2() / 1000d);
		gen.writeEndObject();

		gen.writeEndObject();
	}

}
