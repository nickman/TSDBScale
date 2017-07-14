/**
 * 
 */
package com.heliosapm.tsdbscale.core;

import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanTextMap;
import org.springframework.cloud.sleuth.instrument.web.HttpSpanExtractor;
import org.springframework.cloud.sleuth.util.TextMapUtil;
import org.springframework.util.StringUtils;

/**
 * @author nwhitehead
 *
 */
public class ReactiveServerHttpSpanExtractor implements HttpSpanExtractor {
	
	private static final Random random = new Random(System.currentTimeMillis());
	
	private static final Logger log = LoggerFactory.getLogger(ReactiveServerHttpSpanExtractor.class);
	
	private static final String HEADER_DELIMITER = "-";
	static final String URI_HEADER = "X-Span-Uri";
	private static final String HTTP_COMPONENT = "http";

	/**
	 * {@inheritDoc}
	 * @see org.springframework.cloud.sleuth.SpanExtractor#joinTrace(java.lang.Object)
	 */
	@Override
	public Span joinTrace(SpanTextMap textMap) {
		Map<String, String> carrier = TextMapUtil.asMap(textMap);
		boolean debug = Span.SPAN_SAMPLED.equals(carrier.get(Span.SPAN_FLAGS));
		if (debug) {
			// we're only generating Trace ID since if there's no Span ID will assume
			// that it's equal to Trace ID
			generateIdIfMissing(carrier, Span.TRACE_ID_NAME);
		} else if (carrier.get(Span.TRACE_ID_NAME) == null) {
			// can't build a Span without trace id
			return null;
		}
		try {
			String uri = carrier.get(URI_HEADER);
			boolean skip = Span.SPAN_NOT_SAMPLED.equals(carrier.get(Span.SAMPLED_NAME));
			long spanId = spanId(carrier);
			return buildParentSpan(carrier, uri, skip, spanId);
		} catch (Exception e) {
			log.error("Exception occurred while trying to extract span from carrier", e);
			return null;
		}
	}

	
	private void generateIdIfMissing(Map<String, String> carrier, String key) {
		if (!carrier.containsKey(key)) {
			carrier.put(key, Span.idToHex(random.nextLong()));
		}
	}
	
	private long spanId(Map<String, String> carrier) {
		String spanId = carrier.get(Span.SPAN_ID_NAME);
		if (spanId == null) {
			if (log.isDebugEnabled()) {
				log.debug("Request is missing a span id but it has a trace id. We'll assume that this is "
						+ "a root span with span id equal to the lower 64-bits of the trace id");
			}
			return Span.hexToId(carrier.get(Span.TRACE_ID_NAME));
		} else {
			return Span.hexToId(spanId);
		}
	}
	
	private Span buildParentSpan(Map<String, String> carrier, String uri, boolean skip, long spanId) {
		String traceId = carrier.get(Span.TRACE_ID_NAME);
		Span.SpanBuilder span = Span.builder()
				.traceIdHigh(traceId.length() == 32 ? Span.hexToId(traceId, 0) : 0)
				.traceId(Span.hexToId(traceId))
				.spanId(spanId);
		String processId = carrier.get(Span.PROCESS_ID_NAME);
		String parentName = carrier.get(Span.SPAN_NAME_NAME);
		if (StringUtils.hasText(parentName)) {
			span.name(parentName);
		}  else {
			span.name(HTTP_COMPONENT + ":/parent" + uri);
		}
		if (StringUtils.hasText(processId)) {
			span.processId(processId);
		}
		if (carrier.containsKey(Span.PARENT_ID_NAME)) {
			span.parent(Span.hexToId(carrier.get(Span.PARENT_ID_NAME)));
		}
		span.remote(true);
		boolean debug = Span.SPAN_SAMPLED.equals(carrier.get(Span.SPAN_FLAGS));
		if (debug) {
			span.exportable(true);
		} else if (skip) {
			span.exportable(false);
		}
		for (Map.Entry<String, String> entry : carrier.entrySet()) {
			if (entry.getKey().startsWith(Span.SPAN_BAGGAGE_HEADER_PREFIX + HEADER_DELIMITER)) {
				span.baggage(unprefixedKey(entry.getKey()), entry.getValue());
			}
		}
		return span.build();
	}
	
	private String unprefixedKey(String key) {
		return key.substring(key.indexOf(HEADER_DELIMITER) + 1);
	}
	
	
}
