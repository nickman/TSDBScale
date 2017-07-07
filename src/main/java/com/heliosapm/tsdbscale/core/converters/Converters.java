/**
 * 
 */
package com.heliosapm.tsdbscale.core.converters;

import java.nio.charset.Charset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.pgasync.Converter;
import com.github.pgasync.impl.Oid;
import com.github.pgasync.impl.conversion.ArrayConverter;
import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;
import com.heliosapm.tsdbscale.util.JSONOps;

/**
 * @author nwhitehead
 *
 */
public class Converters {
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	public static class JsonNodeConverter extends AbstractJsonConverter<JsonNode> {
		/**
		 * Creates a new JsonNodeConverter
		 */
		public JsonNodeConverter() {
			super(JsonNode.class, Oid.JSON, Oid.JSONB);
		}
	}
	
	public static class JsonArrayConverter extends AbstractJsonConverter<ArrayNode> {
		/**
		 * Creates a new JsonArrayConverter
		 */
		public JsonArrayConverter() {
			super(ArrayNode.class, Oid.JSON, Oid.JSONB);
		}
	}
	
	public static class JsonObjectConverter extends AbstractJsonConverter<ObjectNode> {
		/**
		 * Creates a new JsonObjectConverter
		 */
		public JsonObjectConverter() {
			super(ObjectNode.class, Oid.JSON, Oid.JSONB);
		}
	}
	
	public static class NumberArrayConverter implements Converter<Number[]> {
	    final Charset UTF8 = Charset.forName("UTF8");

	    public Class<Number[]> type() {
	        return Number[].class;
	    }

	    public byte[] from(Number[] numArr) {
	        return ArrayConverter.fromArray(numArr, o ->  o.toString().getBytes(UTF8));
	    }

	    public Number[] to(Oid oid, byte[] value) {
	        return ArrayConverter.toArray(Number[].class, oid, value, Converters::toNumber);
	    }    
	}
	
	public static class TSDBMetricConverter implements Converter<TSDBMetric[]> {
	    final Charset UTF8 = Charset.forName("UTF8");

	    public Class<TSDBMetric[]> type() {
	        return TSDBMetric[].class;
	    }

	    public byte[] from(TSDBMetric[] metricArr) {
	        return JSONOps.serializeToBytes(metricArr);
	    }

	    public TSDBMetric[] to(Oid oid, byte[] value) {
	        final JsonNode node = JSONOps.parseToNode(value);
	        if(node.isArray()) return JSONOps.parseToObject(node, TSDBMetric[].class);
	        return new TSDBMetric[]{JSONOps.parseToObject(node, TSDBMetric.class)};
	    }    
	}
	
	
    static Number toNumber(Oid oid, byte[] value) {
        String s = new String(value, UTF8);
        if(s.contains(".")) return new Double(s);
        return Long.parseLong(s); 	
    }
	
	

	
	private Converters() {}
}
