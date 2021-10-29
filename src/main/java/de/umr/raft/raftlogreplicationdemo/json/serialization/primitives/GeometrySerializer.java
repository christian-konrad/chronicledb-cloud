package de.umr.raft.raftlogreplicationdemo.json.serialization.primitives;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import de.umr.expressions.translation.util.TemporalGeometry;
import org.locationtech.jts.geom.*;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;

/**
 * 
 */
@JsonComponent
public class GeometrySerializer extends JsonSerializer<Geometry> {
	
	private static final JsonSerializer<CoordinateSequence> CS = new JsonSerializer<>() {

		@Override
		public void serialize(CoordinateSequence value, JsonGenerator gen, SerializerProvider serializers)
				throws IOException {
			if ( value.size() > 1 )
				gen.writeStartArray();
			
			for ( int i = 0; i < value.size(); i++ ) {
				final Coordinate c = value.getCoordinate(i);
				gen.writeStartArray();
				gen.writeNumber(c.x);
				gen.writeNumber(c.y);
				gen.writeEndArray();
			}
			
			if ( value.size() > 1 )
				gen.writeEndArray();
		}
	};

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void serialize(Geometry geometry, JsonGenerator jsonGenerator, SerializerProvider serializers)
			throws IOException {
		jsonGenerator.writeStartObject();
		jsonGenerator.writeStringField("type", geometry.getGeometryType());
		jsonGenerator.writeFieldName("coordinates");
		
		// Special special
		if ( geometry instanceof TemporalGeometry ) {
			geometry = ((TemporalGeometry)geometry).getContainedGeometry();
		}
		
		if ( geometry instanceof Point )
			CS.serialize(((Point) geometry).getCoordinateSequence(), jsonGenerator, serializers);
		else if ( geometry instanceof LineString )
			CS.serialize(((LineString) geometry).getCoordinateSequence(), jsonGenerator, serializers);
		else if ( geometry instanceof Polygon ) {
			final Polygon p = (Polygon) geometry;
			jsonGenerator.writeStartArray();
			CS.serialize(p.getExteriorRing().getCoordinateSequence(), jsonGenerator, serializers);
			for ( int i = 0; i < p.getNumInteriorRing(); i++ ) {
				CS.serialize(p.getInteriorRingN(i).getCoordinateSequence(), jsonGenerator, serializers);
			}
			jsonGenerator.writeEndArray();
		}
		else {
			throw new IllegalArgumentException("Don't know how to serialize geometry: " + geometry.getClass());
		}
		jsonGenerator.writeEndObject();
	}
}
