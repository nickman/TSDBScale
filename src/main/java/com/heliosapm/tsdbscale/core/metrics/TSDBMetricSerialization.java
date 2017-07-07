// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package com.heliosapm.tsdbscale.core.metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.heliosapm.tsdbscale.util.JSONOps;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * <p>Title: TSDBMetricSerialization</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.metrics.TSDBMetricSerialization</code></p>
 */

public class TSDBMetricSerialization {
	/** Sharable TSDBMetric JSON Serializer */
	public static final TSDBMetricJsonSerializer JSON_SERIALIZER = new TSDBMetricJsonSerializer();
	/** Sharable TSDBMetric JSON Deserializer */
	public static final TSDBMetricJsonDeserializer JSON_DESERIALIZER = new TSDBMetricJsonDeserializer();
	/** Sharable TSDBMetric Serializer */
	public static final TSDBMetricSerializer SERIALIZER = new TSDBMetricSerializer();
	/** Sharable TSDBMetric Deserializer */
	public static final TSDBMetricDeserializer DESERIALIZER = new TSDBMetricDeserializer();
	/** Sharable TSDBMetric incoming JSON handler */
	public static final TSDBMetricJsonBytesToMessageHandler INCOMING_HANDLER = new TSDBMetricJsonBytesToMessageHandler();
	/** Sharable TSDBMetric outgoing JSON handler */
	public static final TSDBMetricJsonMessageToBytesHandler OUTGOING_HANDLER = new TSDBMetricJsonMessageToBytesHandler();
	
	public static class TSDBMetricJsonSerializer extends JsonSerializer<TSDBMetric> {

		@Override
		public void serialize(TSDBMetric value, JsonGenerator gen, SerializerProvider serializers)
				throws IOException, JsonProcessingException {
			gen.writeStartObject();
			long id = value.getMetricId();
			if(id > -1) {
				gen.writeNumberField("id", id);
			}
			gen.writeStringField("metric", value.getMetricName());
			gen.writeFieldName("tags");
			gen.writeStartObject();
			for(Map.Entry<String, String> entry: value.getTags().entrySet()) {
				gen.writeStringField(entry.getKey(), entry.getValue());
			}
			gen.writeEndObject();
			gen.writeEndObject();			
		}		
	}
	
	public static class TSDBMetricJsonDeserializer extends JsonDeserializer<TSDBMetric> {
		@Override
		public TSDBMetric deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			JsonNode node = p.readValueAsTree();
			
			Map<String, String> tags = JSONOps.parseToObject(node.get("tags"), JSONOps.TR_STR_STR_HASH_MAP);
			String metricName = node.get("metric").textValue();
			long id = node.get("id").asLong(-1L);
			return new TSDBMetric(metricName, tags, id);
		}
	}
	
	public static class TSDBMetricSerializer implements Serializer<TSDBMetric> {

		@Override
		public byte[] serialize(String topic, TSDBMetric data) {
			return JSONOps.serializeToBytes(data);
		}

		@Override
		public void close() {
			/* No Op */
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			/* No Op */			
		}
	}
	
	public static class TSDBMetricDeserializer implements Deserializer<TSDBMetric> {
		
		@Override
		public TSDBMetric deserialize(String topic, byte[] data) {
			return JSONOps.parseToObject(data, TSDBMetric.class);
		}
		
		@Override
		public void close() {
			/* No Op */
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			/* No Op */			
		}
		
	}
	
	@ChannelHandler.Sharable
	public static class TSDBMetricJsonBytesToMessageHandler extends ByteToMessageDecoder {

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
			final int pos = in.readerIndex();
			try {
				if(in.readableBytes()>2) {
					char firstChar = in.getChar(0);
					if(firstChar=='{') {
						out.add(JSONOps.parseToObject(in, TSDBMetric.class));
					} else if(firstChar=='[') {
						Collections.addAll(out, JSONOps.parseToObject(in, TSDBMetric[].class));
					}
				}
			} catch (IndexOutOfBoundsException ex) {
				in.readerIndex(pos);
				throw ex;
			}
		}		
	}
	
	@ChannelHandler.Sharable
	public static class TSDBMetricJsonMessageToBytesHandler extends MessageToByteEncoder<TSDBMetric> {

		@Override
		protected void encode(ChannelHandlerContext ctx, TSDBMetric msg, ByteBuf out) throws Exception {
			JSONOps.serialize(msg, out);
		}
		
	}
	
	private TSDBMetricSerialization() {}

}
