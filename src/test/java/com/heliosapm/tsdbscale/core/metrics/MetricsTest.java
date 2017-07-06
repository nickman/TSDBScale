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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.heliosapm.tsdbscale.util.JSONOps;

/**
 * <p>Title: MetricsTest</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.MetricsTest</code></p>
 */

public class MetricsTest {
	
	

	@Test
	public void testBaseTags() {
		TSDBMetric metric = new TSDBMetric("foo", "app", "appX", "host", "hostY", "dc", "dcW", "bbB", "whoKnows", "aaA", "someValue");
		System.out.println(metric);
		Assert.assertEquals("metric name", "foo", metric.getMetricName());
		int pair = 0;
		for(Map.Entry<String, String> entry: metric.getTags().entrySet()) {
			switch(pair) {
			case 0:
				Assert.assertEquals("tag key [" + pair + "]", "dc", entry.getKey());
				Assert.assertEquals("tag key [" + pair + "]", "dcw", entry.getValue());
				break;
			case 1:
				Assert.assertEquals("tag key [" + pair + "]", "host", entry.getKey());
				Assert.assertEquals("tag key [" + pair + "]", "hosty", entry.getValue());
				break;
			case 2:
				Assert.assertEquals("tag key [" + pair + "]", "app", entry.getKey());
				Assert.assertEquals("tag key [" + pair + "]", "appx", entry.getValue());
				break;
			case 3:
				Assert.assertEquals("tag key [" + pair + "]", "aaa", entry.getKey());
				Assert.assertEquals("tag key [" + pair + "]", "somevalue", entry.getValue());
				break;
			case 4:
				Assert.assertEquals("tag key [" + pair + "]", "bbb", entry.getKey());
				Assert.assertEquals("tag key [" + pair + "]", "whoknows", entry.getValue());
				break;
				
			default:
				Assert.fail("Unexpected pair:" + pair);				
			}
			pair++;
		}
	}
	
	@Test
	public void testJsonSer() {
		TSDBMetric metric = new TSDBMetric("foo", "app", "appX", "host", "hostY", "dc", "dcW", "bbB", "whoKnows", "aaA", "someValue");
		String jsonText = JSONOps.serializeToString(metric);
		String expected = "{\"metric\":\"foo\",\"tags\":{\"dc\":\"dcw\",\"host\":\"hosty\",\"app\":\"appx\",\"aaa\":\"somevalue\",\"bbb\":\"whoknows\"}}";
		System.out.println(jsonText);
		Assert.assertEquals("metric json", expected, jsonText);
	}
	
	@Test
	public void testJsonDeser() {
		String jsonText = "{\"metric\":\"foo\",\"tags\":{\"dc\":\"dcw\",\"host\":\"hosty\",\"app\":\"appx\",\"aaa\":\"somevalue\",\"bbb\":\"whoknows\"}}";
		TSDBMetric matchingMetric = new TSDBMetric("foo", "app", "appX", "host", "hostY", "dc", "dcW", "bbB", "whoKnows", "aaA", "someValue");
		TSDBMetric nonMatchingMetric = new TSDBMetric("foo", "app", "appX", "host", "hostY", "dc", "dcW", "bbB", "whoKns", "aaA", "someValue");
		TSDBMetric deserMetric = JSONOps.parseToObject(jsonText, TSDBMetric.class);
		Assert.assertEquals("matching deser metric", matchingMetric, deserMetric);
		Assert.assertNotEquals("non matching deser metric", nonMatchingMetric, deserMetric);
	}
	
	@Test
	public void testSerializer() {
		TSDBMetric metric = new TSDBMetric("foo", "app", "appX", "host", "hostY", "dc", "dcW", "bbB", "whoKnows", "aaA", "someValue");
		byte[] bytes = TSDBMetricSerialization.SERIALIZER.serialize("topic", metric);
		TSDBMetric metric2 = TSDBMetricSerialization.DESERIALIZER.deserialize("topic", bytes);
		Assert.assertEquals("matching deser metric", metric, metric2);
	}
	
	@Test
	public void testMessageToBytes() {
		
	}
}
