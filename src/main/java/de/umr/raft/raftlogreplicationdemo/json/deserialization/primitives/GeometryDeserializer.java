package de.umr.raft.raftlogreplicationdemo.json.deserialization.primitives;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;


/**
 * 
 */
@JsonComponent
public class GeometryDeserializer extends JsonDeserializer<Geometry> {
	
	private static final GeometryFactory GF = new GeometryFactory();
	
	private static final JsonDeserializer<CoordinateSequence> CS_DESERIALIZER = new JsonDeserializer<>() {

		@Override
		public CoordinateSequence deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			JsonNode node = p.getCodec().readTree(p);
			
			final int numCoords = node.size();
			
			final CoordinateSequence cs = new CoordinateArraySequence(numCoords);
			for (int i = 0; i < numCoords; i++) {
				JsonNode cNode = node.get(i);
				cs.getCoordinate(i).x = cNode.get(0).asDouble();
				cs.getCoordinate(i).y = cNode.get(1).asDouble();
			}
			return cs;
		}
		
	};

	/**
	 * @{inheritDoc}
	 */
	@Override
	public Geometry deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		JsonNode node = p.getCodec().readTree(p);
		
		switch ( node.get("type").asText().toLowerCase() ) {
			case "point":
				return createPoint(node.get("coordinates"));
			case "linestring":
				return createLineString(node, p, ctxt);
			case "polygon":
				return createPolygon(node,p,ctxt);
			default:
				throw new IllegalArgumentException("Invalid geometry type: " + node.get("type").asText());
		}
	}
	
	private static Point createPoint(JsonNode coords) {
		switch ( coords.size() ) {
			case 0:
				return GF.createPoint();
			case 2:
				return GF.createPoint(new Coordinate(coords.get(0).asDouble(), coords.get(1).asDouble()));
			default:
				throw new IllegalArgumentException("Illegal coordinates for point: " + coords.asText());
		}
	}
	
	private static LineString createLineString(JsonNode coords, JsonParser p, DeserializationContext ctxt) throws JsonProcessingException, IOException {
		switch ( coords.size() ) {
			case 0:
				return GF.createLineString();
			case 1:
				throw new IllegalArgumentException("LineStrings require at least two coordinates.");
			default:
				return GF.createLineString( CS_DESERIALIZER.deserialize(coords.traverse(p.getCodec()), ctxt) );
		}
	}
	
	private static Polygon createPolygon(JsonNode coords, JsonParser p, DeserializationContext ctxt) throws JsonProcessingException, IOException {
		if ( coords.size() == 0 ) 
			return GF.createPolygon();
		
		LinearRing[] inner = new LinearRing[coords.size()-1];
		LinearRing shell = GF.createLinearRing(CS_DESERIALIZER.deserialize(coords.get(0).traverse(p.getCodec()), ctxt));

		for ( int i = 1; i < coords.size(); i++ ) {
			inner[i-1] = GF.createLinearRing(CS_DESERIALIZER.deserialize(coords.get(i).traverse(p.getCodec()), ctxt));
		}
		return GF.createPolygon(shell, inner);
	}

}
