package com.heliosapm.tsdbscale.core.trace;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * <p>Title: TraceSpanTest</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.trace.TraceSpanTest</code></p>
 */
public class TraceSpanTest {

	public static final Map<String, String> httpHeaderMap() {
		final Map<String, String> httpHeaderMap = new HashMap<>();
		httpHeaderMap.put("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8");
		httpHeaderMap.put("Accept-Encoding", "gzip, deflate, br");
		httpHeaderMap.put("Accept-Language", "en-US,en;q=0.8");
		httpHeaderMap.put("Cache-Control", "no-cache");
		httpHeaderMap.put("Connection", "keep-alive");
		httpHeaderMap.put("Cookie", "TWISTED_SESSION=c1eaa942698bebe52df67b687cbc54fa");
		httpHeaderMap.put("Host", "localhost");
		httpHeaderMap.put("Pragma", "no-cache");
		httpHeaderMap.put("Upgrade-Insecure-Requests", "1");
		httpHeaderMap.put("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36");
		httpHeaderMap.put("X-B3-Flags", "1");
		httpHeaderMap.put("X-B3-Sampled", "1");
		httpHeaderMap.put("X-B3-SpanId", "015f636be42d2d71");
		httpHeaderMap.put("X-B3-TraceId", "015f636be42d2d71");
		httpHeaderMap.put("X-Zipkin-Extension", "1");
		return httpHeaderMap;
	}
	
	public static final Map<String, String> httpHeaderMapWithBaggage() {
		final Map<String, String> httpHeaderMap = httpHeaderMap();
		httpHeaderMap.put("baggage-ABC", "sna-fu");
		httpHeaderMap.put("baggage-XYZ", "foo-bar");
		return httpHeaderMap;

	}
	
	
	@Test
	public void test_http_headers_span() {
		final Map<String, String> httpHeaderMap = httpHeaderMap();
		
		
	}
}


//Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
//Accept-Encoding:gzip, deflate, br
//Accept-Language:en-US,en;q=0.8
//Cache-Control:no-cache
//Connection:keep-alive
//Cookie:TWISTED_SESSION=c1eaa942698bebe52df67b687cbc54fa
//Host:localhost:8888
//Pragma:no-cache
//Upgrade-Insecure-Requests:1
//User-Agent:Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36
//X-B3-Flags:1
//X-B3-Sampled:1
//X-B3-SpanId:015f636be42d2d71
//X-B3-TraceId:015f636be42d2d71
//X-Zipkin-Extension:1

