package de.umr.raft.raftlogreplicationdemo.json.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.json.serialization.event.EventSerializer;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.InsertEventRequest;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedChronicleEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;


// TODO start again once cluster management works

@JsonComponent
public class InsertEventRequestSerializer extends JsonSerializer<InsertEventRequest> {

	private final ReplicatedChronicleEngine engine;
	
	/**
	 * Creates a new InsertRequestDeserializer instance
	 * @param engine
	 */
	@Autowired
	public InsertEventRequestSerializer(ReplicatedChronicleEngine engine) {
		this.engine = engine;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void serialize(InsertEventRequest value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
//		final EventSchema schema = engine.getSchema(value.getStreamName());
//		final EventSerializer es = new EventSerializer(schema);
//
//		gen.writeStartObject();
//		gen.writeStringField("streamName", value.getStreamName());
//		gen.writeArrayFieldStart("events");
//		for ( Event e : value.getEvents() ) {
//			es.serialize(e, gen, serializers);
//		}
//		gen.writeEndArray();
//		gen.writeEndObject();
	}
}
