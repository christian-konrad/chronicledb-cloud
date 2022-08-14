package de.umr.raft.raftlogreplicationdemo.json.deserialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import de.umr.event.Event;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.json.deserialization.event.EventDeserializer;
import de.umr.raft.raftlogreplicationdemo.models.eventstore.InsertEventRequest;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.BaseChronicleEngine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.EmbeddedChronicleEngine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedChronicleEngine;
import de.umr.raft.raftlogreplicationdemo.replication.impl.facades.eventstore.ReplicatedEventStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jackson.JsonComponent;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 
 */
@JsonComponent
public class InsertEventRequestDeserializer extends JsonDeserializer<InsertEventRequest> {

	private final ReplicatedChronicleEngine engine;
	private final HttpServletRequest request;
	
	/**
	 * Creates a new InsertRequestDeserializer instance
	 * @param engine
	 */
	@Autowired
	public InsertEventRequestDeserializer(ReplicatedChronicleEngine engine, HttpServletRequest request) {
		// TODO different engines, depending on mapping
		this.engine = engine;
		this.request = request;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public InsertEventRequest deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		JsonNode node = p.getCodec().readTree(p);

		// inject the path param
		Map map = (Map) request.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);

		String streamName = (String) map.get("streamName");

		final EventSchema schema = engine.getSchema(streamName);
		final EventDeserializer ed = new EventDeserializer(schema);
		
		JsonNode eventsNode = node.get("events");
		List<Event> events = new ArrayList<>(eventsNode.size());
		
		for ( JsonNode eventNode : eventsNode ) {
			events.add( ed.deserialize(eventNode.traverse(p.getCodec()), ctxt));
		}
		return new InsertEventRequest(events);
	}

}
