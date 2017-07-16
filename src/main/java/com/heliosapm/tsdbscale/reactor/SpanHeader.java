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
package com.heliosapm.tsdbscale.reactor;

import java.util.EnumMap;
import java.util.Map;

import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Span.SpanBuilder;

/**
 * <p>Title: SpanHeader</p>
 * <p>Description: Functional enumeration of span headers</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.reactor.SpanHeader</code></p>
 * 
 */


public enum SpanHeader implements SpanBuilderFieldSetter {
	
	PROCESS_ID("X-Process-Id", new ProcessIdSetter()),
	PARENT_ID("X-B3-ParentSpanId", new ParentSetter()),
	TRACE_ID("X-B3-TraceId", new TraceIdSetter()),
	SPAN_NAME("X-Span-Name", new NameSetter()),
	SPAN_ID("X-B3-SpanId", new SpanIdSetter()),
	SPAN_EXPORT("X-Span-Export", new ExportableSetter()),	
	SPAN_REMOTE("X-Span-Remote", new RemoteSetter()),
	SPAN_BAGGAGE_HEADER_PREFIX("baggage", new BaggageSetter("baggage-")),
	SPAN_LOG_HEADER_PREFIX("X-B3-Log", new LogSetter("X-B3-Log-")),
	SPAN_TAG_HEADER_PREFIX("X-B3-Tag", new TagSetter("X-B3-Tag-")),

	
	
	URI_HEADER("X-Span-Uri", null),
	SAMPLED("X-B3-Sampled", null),
	SPAN_FLAGS("X-B3-Flags", null);
	

	
// ============================================================================
//		Standard SpanBuilder
// ============================================================================	
//	public static class NameSetter implements SpanBuilderFieldSetter {
//	public static class TraceIdSetter implements SpanBuilderFieldSetter {
//	public static class ProcessIdSetter implements SpanBuilderFieldSetter {
//	public static class BaggageSetter implements SpanBuilderFieldSetter {	
//	public static class ExportableSetter implements SpanBuilderFieldSetter {
//	public static class SpanIdSetter implements SpanBuilderFieldSetter {	
//	public static class ParentSetter implements SpanBuilderFieldSetter {
//	public static class RemoteSetter implements SpanBuilderFieldSetter {	
	
// ============================================================================
//	Standard Span
//============================================================================	
//	public static class LogSetter implements SpanBuilderFieldSetter {
//	public static class TagSetter implements SpanBuilderFieldSetter {

	
// ============================================================================
//		Not Sure
//============================================================================	
//	public static class TraceIdHighSetter implements SpanBuilderFieldSetter {
//	public static class ParentsSetter implements SpanBuilderFieldSetter {
//	public static class BaggagesSetter implements SpanBuilderFieldSetter {
//	public static class TagsSetter implements SpanBuilderFieldSetter {
//	public static class LogsSetter implements SpanBuilderFieldSetter {	



	
	private static final SpanHeader[] values = values();

	private SpanHeader(final String value, final SpanBuilderFieldSetter spanSetter) {
		this(value, spanSetter, false, false, false, false);
	}

	
	
	private SpanHeader(final String value, final SpanBuilderFieldSetter spanSetter, final boolean spanField, 
			final boolean tag, final boolean log, final boolean baggage) {
		this.value = value;
		this.spanSetter = spanSetter;
		this.spanField = spanField;
		this.tag = tag;
		this.log = log;
		this.baggage = baggage;
		
	}
	
	public final String value;
	public final boolean spanField; 
	public final boolean tag;
	public final boolean log;
	public final boolean baggage;
	public final SpanBuilderFieldSetter spanSetter;

	@Override
	public SpanBuilder setSpanBuilderField(SpanBuilder builder, Object value, String... keys) {
		if(spanSetter!=null) {
			spanSetter.setSpanBuilderField(builder, value, keys);
		}
		return builder;
	}

	public static Span buildSpan(final Map<String, String> map) {
		if(map==null || map.isEmpty()) return null;
		final Map<SpanHeader, Object> headers = new EnumMap<SpanHeader, Object>(SpanHeader.class);
		SpanBuilder builder = Span.builder();
		for(Map.Entry<String, String> entry : map.entrySet()) {
			for(SpanHeader header : values) {
				//header.setSpanBuilderField(builder, value, keys)
			}
		}
		return builder.build();
	}
	
	public static void put(final Map<String, String> map, final Map<SpanHeader, Object> headers, final SpanHeader sh) {
		String value = map.get(sh.value);
		if(value!=null) {
			Object obj = sh.apply(value.trim());
			if(obj!=null) {
				headers.put(sh, obj);
			}
		}
	}
//	Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
//			Accept-Encoding:gzip, deflate, br
//			Accept-Language:en-US,en;q=0.8
//			Cache-Control:no-cache
//			Connection:keep-alive
//			Cookie:TWISTED_SESSION=c1eaa942698bebe52df67b687cbc54fa
//			Host:localhost:8888
//			Pragma:no-cache
//			Upgrade-Insecure-Requests:1
//			User-Agent:Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
//			X-B3-Flags:1
//			X-B3-Sampled:1
//			X-B3-SpanId:015f636be42d2d71
//			X-B3-TraceId:015f636be42d2d71
//			X-Zipkin-Extension:1
//			
				
	
	
}



