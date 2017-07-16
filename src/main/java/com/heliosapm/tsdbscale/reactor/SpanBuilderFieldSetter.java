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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.springframework.cloud.sleuth.Log;
import org.springframework.cloud.sleuth.Span.SpanBuilder;

import com.heliosapm.tsdbscale.util.JSONOps;

/**
 * <p>Title: SpanBuilderFieldSetter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.reactor.SpanBuilderFieldSetter</code></p>
 */

public interface SpanBuilderFieldSetter extends Function<Object, Object> {
	public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map);

	@Override
	default Object apply(Object t) {
		if(t==null) return null;
		return t.toString().trim();
	}
	
	public String getHeaderName();
	
	public abstract static class AbstractSetter implements SpanBuilderFieldSetter {
		private final String headerName;

		public AbstractSetter(String headerName) {
			this.headerName = headerName;
		}
		
		public String getHeaderName() {
			return headerName;
		}
		
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			if(map.isEmpty()) return builder;
			Object o = map.get(headerName);
			if(o!=null) {
				setField(builder, o);
			}
			return builder;
		}
		
		protected abstract void setField(SpanBuilder builder, Object value);
	}
	
	public abstract static class AbstractStringSetter implements SpanBuilderFieldSetter {
		private final String headerName;

		public AbstractStringSetter(String headerName) {
			this.headerName = headerName;
		}
		
		public String getHeaderName() {
			return headerName;
		}
		
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			if(map.isEmpty()) return builder;
			Object o = map.get(headerName);
			if(o!=null) {
				setField(builder, o.toString().trim());
			}
			return builder;
		}
		
		protected abstract void setField(SpanBuilder builder, String value);
	}
	

	public abstract static class AbstractLongSetter implements SpanBuilderFieldSetter {
		private final String headerName;

		public AbstractLongSetter(String headerName) {
			this.headerName = headerName;
		}
		
		public String getHeaderName() {
			return headerName;
		}
		
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			if(map.isEmpty()) return builder;
			Object o = map.get(headerName);
			if(o!=null) {
				setField(builder, Long.parseLong(o.toString().trim()));
			}
			return builder;
		}
		
		@Override
		public Object apply(Object t) {
			if(t==null) return null;
			return Long.parseLong(t.toString().trim());
		}
		
		
		protected abstract void setField(SpanBuilder builder, long value);
	}
	
	
	public abstract static class AbstractMultiSetter implements SpanBuilderFieldSetter {
		private final String headerName;

		public AbstractMultiSetter(String headerName) {
			this.headerName = headerName;
		}
		
		public String getHeaderName() {
			return headerName;
		}
		
		
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			if(map.isEmpty()) return builder;
			Map<String, Object> unprefixedMap = new HashMap<>();
			for(Map.Entry<String, Object> entry: map.entrySet()) {
				if(entry.getKey().indexOf(headerName + "-")==0) {
					unprefixedMap.put(TraceUtils.unprefixedKey(entry.getKey(), headerName + "-"), entry.getValue());
				}
			}
			if(!unprefixedMap.isEmpty()) {
				for(Map.Entry<String, Object> entry: unprefixedMap.entrySet()) {
					setField(builder, entry.getKey(), entry.getValue());
				}				
			}			
			return builder;
		}
		
		protected abstract void setField(SpanBuilder builder, String key, Object value);
	}
	
	
	public static class BeginSetter extends AbstractLongSetter {
		public BeginSetter(String headerName) {
			super(headerName);
		}

		@Override
		protected void setField(SpanBuilder builder, long value) {
			builder.begin(value);
			
		}

	}
	
	public static class EndSetter extends AbstractLongSetter {
		public EndSetter(String headerName) {
			super(headerName);
		}

		@Override
		protected void setField(SpanBuilder builder, long value) {
			builder.end(value);			
		}

	}
	
	
	
	public static class NameSetter extends AbstractStringSetter {
		
		public NameSetter(String headerName) {
			super(headerName);
		}
		
		@Override
		protected void setField(SpanBuilder builder, String value) {
			builder.name(value);
		}
		
	}
	
	public static class TraceIdHighSetter extends AbstractLongSetter {
		public TraceIdHighSetter(String headerName) {
			super(headerName);
		}

		@Override
		protected void setField(SpanBuilder builder, long value) {
			builder.traceIdHigh(value);			
		}
	}

	
	public static class TraceIdSetter extends AbstractLongSetter {
		public TraceIdSetter(String headerName) {
			super(headerName);
		}
		
		@Override
		protected void setField(SpanBuilder builder, long value) {
			builder.traceId(value);			
		}
	}
	
	
	public static class ParentSetter extends AbstractLongSetter {
		public ParentSetter(String headerName) {
			super(headerName);
		}
		
		@Override
		protected void setField(SpanBuilder builder, long value) {
			builder.parent(value);			
		}
	}
	
	public static class ParentsSetter extends AbstractSetter {
		
		public ParentsSetter(String headerName) {
			super(headerName);
		}
		
		@Override
		protected void setField(SpanBuilder builder, Object value) {
			
			
		}

		
		@SuppressWarnings("unchecked")
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			final Set<Long> ids = new LinkedHashSet<Long>(); 
			for(String s: TraceUtils.splitString(value.toString().trim(), ',', true)) {
				ids.add(Long.parseLong(s));
			}
			builder.parents((Set<Long>)apply(value));
			return builder;
		}		
		@Override
		public Object apply(Object t) {
			if(t==null) return null;
			final Set<Long> ids = new LinkedHashSet<Long>(); 
			for(String s: TraceUtils.splitString(t.toString().trim(), ',', true)) {
				ids.add(Long.parseLong(s));
			}
			return ids;			
		}
		
	}
	
	
//	public static class LogsSetter implements SpanBuilderFieldSetter {
//		@SuppressWarnings("unchecked")
//		@Override
//		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
//			builder.logs((Set<Log>)apply(value));
//			return builder;
//		}
//		@Override
//		public Object apply(Object t) {
//			if(t==null) return null;
//			String[] ls = TraceUtils.splitString(t.toString().trim(), ',', true);
//			Set<Log> logs = new LinkedHashSet<Log>();
//			for(int i = 0; i < ls.length; i++) {
//				try {
//					long l = Long.parseLong(ls[i]);
//					i++;
//					String s = ls[i];
//					logs.add(new Log(l,s));
//				} catch (Exception ex) {}
//			}
//			return logs;
//		}		
//	}
	
	
//	public static class TagsSetter implements SpanBuilderFieldSetter {
//		@Override
//		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
//			Map<String, String> map = TraceUtils.fromString(value.toString());
//			builder.tags(map);
//			return builder;
//		}		
//		@Override
//		public Object apply(Object t) {
//			if(t==null) return null;
//			return TraceUtils.fromString(t.toString().trim());
//		}		
//	}
	
	
	/*
	 * Baggage:  	baggage-X=FOO
	 * Baggages: 	baggage-X=FOO,baggage-Y=BAR
	 * 
	 * Tag:  		tag-X=FOO
	 * Tags: 		tag-X=FOO,tag-Y=BAR
	 * 
	 * Log:			log-X=FOO   or  log-X=123:FOO 
	 * Logs:		log-X=FOO,log-Y=BAR   or  log-X=123:FOO,log-Y=123:BAR
	 * 
	 * 
	 */
	
	
	public static abstract class PrefixedKeySetter implements SpanBuilderFieldSetter {
		final String prefix;

		
		public PrefixedKeySetter(String prefix) {
			this.prefix = prefix;			
		}
		
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			applyToSpanBuilder(builder, collect(value));
			return builder;
		}		
		
		protected Object collect(Object value) {
			return TraceUtils.removeKeyPrefix(TraceUtils.fromString(value.toString().trim()), prefix);
		}
		
		protected abstract void applyToSpanBuilder(SpanBuilder builder, Object value);
		
	}
	
	public static class BaggageSetter extends PrefixedKeySetter {

		public BaggageSetter(final String prefix) {
			super(prefix);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void applyToSpanBuilder(SpanBuilder builder, Object value) {
			if(value instanceof Map) {
				Map<String, String> map = (Map<String, String>) value;
				if(!map.isEmpty()) {
					builder.baggage(map);
				}
			}			
		}
		
	}
	
	public static class TagSetter extends PrefixedKeySetter {

		public TagSetter(final String prefix) {
			super(prefix);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void applyToSpanBuilder(SpanBuilder builder, Object value) {
			if(value instanceof Map) {
				Map<String, String> map = (Map<String, String>) value;
				if(!map.isEmpty()) {
					builder.tags(map);
				}
			}			
		}
		
	}
	
	
	
	/*
	 * Log:			log-X=FOO   or  log-X=123:FOO 
	 * Logs:		log-X=FOO,Y=BAR   or  log-X=123:FOO,123:BAR
	 * or json
	 */
	
	public static class LogSetter extends PrefixedKeySetter {

		public LogSetter(final String prefix) {
			super(prefix);
		}
		
		@Override
		protected void applyToSpanBuilder(SpanBuilder builder, Object value) {
			Collection<Log> logs = (Collection)collect(value);
			if(!logs.isEmpty()) {
				builder.logs(logs);
			}
		}
		
		protected Object collect(Object value) {
			final String s = value.toString().trim();
			char firstChar = s.charAt(0);
			if(firstChar=='[') {
				Log[] logs = JSONOps.parseToObject(s, Log[].class);
				return new LinkedHashSet<Log>(Arrays.asList(logs));
			} else if(firstChar=='{') {
				if(s.replace(" ", "").charAt(1)=='"') {
					return Collections.singleton(JSONOps.parseToObject(s, Log.class));
				} else {					
					Set<Log> logs = new LinkedHashSet<>();
					for(Map.Entry<String, String> entry: TraceUtils.fromString(s).entrySet()) {
						// We discard the key
						String v = entry.getValue();
						for(String logVal: TraceUtils.splitString(v, ',', true)) {
							int index = logVal.indexOf(':');
							if(index!=-1) {
								String[] tval = TraceUtils.splitString(logVal, ':', true);
								logs.add(new Log(Long.parseLong(tval[0]), tval[1]));
							} else {
								logs.add(new Log(System.currentTimeMillis(), logVal));
							}
						}
					}
					return logs;
				}
			}
			return null;
		}

		
	}

	
	
	public static class SpanIdSetter implements SpanBuilderFieldSetter {
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			builder.spanId(Long.parseLong(value.toString().trim()));
			return builder;
		}		
		@Override
		public Object apply(Object t) {
			if(t==null) return null;
			return Long.parseLong(t.toString().trim());
		}		
	}
	
	public static class RemoteSetter implements SpanBuilderFieldSetter {
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			builder.remote(Boolean.parseBoolean(value.toString().trim()));
			return builder;
		}
		@Override
		public Object apply(Object t) {
			if(t==null) return null;
			return Boolean.parseBoolean(t.toString().trim());
		}		
	}
	
	public static class ExportableSetter implements SpanBuilderFieldSetter {
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			builder.exportable(Boolean.parseBoolean(value.toString().trim()));
			return builder;
		}
		@Override
		public Object apply(Object t) {
			if(t==null) return null;
			return Boolean.parseBoolean(t.toString().trim());
		}				
	}
	
	public static class ProcessIdSetter implements SpanBuilderFieldSetter {
		@Override
		public SpanBuilder setSpanBuilderField(SpanBuilder builder, Map<String, Object> map) {
			builder.processId(value.toString().trim());
			return builder;
		}		
	}
	
	
	
	
}



