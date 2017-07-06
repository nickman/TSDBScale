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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.heliosapm.tsdbscale.core.metrics.TSDBMetricSerialization.TSDBMetricJsonDeserializer;
import com.heliosapm.tsdbscale.core.metrics.TSDBMetricSerialization.TSDBMetricJsonSerializer;
import com.heliosapm.tsdbscale.util.JSONOps;

/**
 * <p>Title: TSDBMetric</p>
 * <p>Description: A TSDB metric representation</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.metrics.TSDBMetric</code></p>
 */

@JsonSerialize(using=TSDBMetricJsonSerializer.class)
@JsonDeserialize(using=TSDBMetricJsonDeserializer.class)
public class TSDBMetric {
	/** The metric name */
	private final String metricName;
	/** The tags */
	private final SortedMap<String, String> tags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
	
	static {
		JSONOps.registerDeserializer(TSDBMetric.class, new TSDBMetricJsonDeserializer());
		JSONOps.registerSerializer(TSDBMetric.class, new TSDBMetricJsonSerializer());
	}
	
	
	/**
	 * Creates a new TSDBMetric
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public TSDBMetric(final String metricName, final Map<String, String> tags) {
		Objects.requireNonNull(metricName, "metric name was null");
		Objects.requireNonNull(tags, "tags was null");		
		this.metricName = metricName.trim().toLowerCase();
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			this.tags.put(entry.getKey().trim().toLowerCase(), entry.getValue().trim().toLowerCase());
		}
		// TODO: add default tags if not present
	}
	
	/**
	 * Creates a new TSDBMetric
	 * @param metricName The metric name
	 * @param tags The metric tags as a series of key/value pairs
	 */
	public TSDBMetric(final String metricName, final String... tags) {
		Objects.requireNonNull(metricName, "metric name was null");
		Objects.requireNonNull(tags, "tags was null");		
		final int tagLen = tags.length;
		if(tagLen%2!=0) throw new IllegalArgumentException("Uneven number of tags:" + tagLen);
		this.metricName = metricName.trim().toLowerCase();
		for(int i = 0; i < tagLen; i++) {
			String key = tags[i].trim().toLowerCase();
			i++;
			String value = tags[i].trim().toLowerCase();
			this.tags.put(key, value);
		}
	}
	


	/**
	 * Returns the metric name
	 * @return the metricName
	 */
	public String getMetricName() {
		return metricName;
	}


	/**
	 * Returns the tags
	 * @return the tags
	 */
	public SortedMap<String, String> getTags() {
		return Collections.unmodifiableSortedMap(tags);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return new StringBuilder(metricName)
			.append(":")
			.append(tags)
			.toString();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((metricName == null) ? 0 : metricName.hashCode());
		result = prime * result + ((tags == null) ? 0 : tags.hashCode());
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TSDBMetric other = (TSDBMetric) obj;
		if (metricName == null) {
			if (other.metricName != null)
				return false;
		} else if (!metricName.equals(other.metricName))
			return false;
		if (tags == null) {
			if (other.tags != null)
				return false;
		} else if (!tags.equals(other.tags))
			return false;
		return true;
	}

}
