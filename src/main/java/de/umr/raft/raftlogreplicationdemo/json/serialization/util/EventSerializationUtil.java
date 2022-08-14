package de.umr.raft.raftlogreplicationdemo.json.serialization.util;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.deser.std.NumberDeserializers.*;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.ser.std.BooleanSerializer;
import com.fasterxml.jackson.databind.ser.std.NumberSerializers.*;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import de.umr.chronicledb.common.util.KeyValue;
import de.umr.event.schema.Attribute;
import de.umr.event.schema.EventSchema;
import de.umr.raft.raftlogreplicationdemo.json.deserialization.primitives.GeometryDeserializer;
import de.umr.raft.raftlogreplicationdemo.json.serialization.primitives.GeometrySerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public class EventSerializationUtil {

	public static final String					TSTART					= "tstart";
	public static final String					TEND					= "tend";

	public static final BooleanSerializer		BOOLEAN_SERIALIZER		= new BooleanSerializer(false);
	public static final IntLikeSerializer		BYTE_SERIALIZER			= new IntLikeSerializer();
	public static final ShortSerializer			SHORT_SERIALIZER		= new ShortSerializer();
	public static final IntegerSerializer		INT_SERIALIZER			= new IntegerSerializer(Integer.class);
	public static final LongSerializer			LONG_SERIALIZER			= new LongSerializer(Long.class);
	public static final FloatSerializer			FLOAT_SERIALIZER		= new FloatSerializer();
	public static final DoubleSerializer		DOUBLE_SERIALIZER		= new DoubleSerializer(Double.class);
	public static final StringSerializer		STRING_SERIALIZER		= new StringSerializer();
	public static final GeometrySerializer		GEOMETRY_SERIALIZER		= new GeometrySerializer();

	public static final BooleanDeserializer		BOOLEAN_DESERIALIZER	= new BooleanDeserializer(Boolean.class, null);
	public static final ByteDeserializer		BYTE_DESERIALIZER		= new ByteDeserializer(Byte.class, null);
	public static final ShortDeserializer		SHORT_DESERIALIZER		= new ShortDeserializer(Short.class, null);
	public static final IntegerDeserializer		INT_DESERIALIZER		= new IntegerDeserializer(Integer.class, null);
	public static final LongDeserializer		LONG_DESERIALIZER		= new LongDeserializer(Long.class, null);
	public static final FloatDeserializer		FLOAT_DESERIALIZER		= new FloatDeserializer(Float.class, null);
	public static final DoubleDeserializer		DOUBLE_DESERIALIZER		= new DoubleDeserializer(Double.class, null);
	public static final StringDeserializer		STRING_DESERIALIZER		= new StringDeserializer();
	public static final GeometryDeserializer	GEOMETRY_DESERIALIZER	= new GeometryDeserializer();

	public static List<KeyValue<String, JsonSerializer<Object>>> getAttributeSerializers(EventSchema schema) {
		List<KeyValue<String, JsonSerializer<Object>>> result = new ArrayList<>(schema.getNumAttributes());
		for (Attribute a : schema) {
			result.add(new KeyValue<>(a.getName(), getAttributeSerializer(a)));
		}
		return result;
	}
	
	public static List<KeyValue<String, JsonDeserializer<?>>> getAttributeDeserializers(EventSchema schema) {
		List<KeyValue<String, JsonDeserializer<?>>> result = new ArrayList<>(schema.getNumAttributes());
		for (Attribute a : schema) {
			result.add(new KeyValue<>(a.getName(), getAttributeDeserializer(a)));
		}
		return result;
	}
	

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static JsonSerializer<Object> getAttributeSerializer(Attribute attribute) {
		switch (attribute.getType()) {
			case BOOLEAN:
				return BOOLEAN_SERIALIZER;
			case BYTE:
				return BYTE_SERIALIZER;
			case SHORT:
				return SHORT_SERIALIZER;
			case INTEGER:
				return INT_SERIALIZER;
			case LONG:
				return LONG_SERIALIZER;
			case FLOAT:
				return FLOAT_SERIALIZER;
			case DOUBLE:
				return DOUBLE_SERIALIZER;
			case STRING:
				return STRING_SERIALIZER;
			case GEOMETRY:
				return (JsonSerializer) GEOMETRY_SERIALIZER;
			default:
				throw new IllegalArgumentException("Unsupported data type: " + attribute.getType());
		}
	}
	
	private static JsonDeserializer<?> getAttributeDeserializer(Attribute attribute) {
		switch (attribute.getType()) {
			case BOOLEAN:
				return BOOLEAN_DESERIALIZER;
			case BYTE:
				return BYTE_DESERIALIZER;
			case SHORT:
				return SHORT_DESERIALIZER;
			case INTEGER:
				return INT_DESERIALIZER;
			case LONG:
				return LONG_DESERIALIZER;
			case FLOAT:
				return FLOAT_DESERIALIZER;
			case DOUBLE:
				return DOUBLE_DESERIALIZER;
			case STRING:
				return STRING_DESERIALIZER;
			case GEOMETRY:
				return GEOMETRY_DESERIALIZER;
			default:
				throw new IllegalArgumentException("Unsupported data type: " + attribute.getType());
		}
	}

}
