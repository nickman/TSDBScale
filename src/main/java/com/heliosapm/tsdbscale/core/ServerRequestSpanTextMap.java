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
package com.heliosapm.tsdbscale.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.springframework.cloud.sleuth.SpanTextMap;
import org.springframework.web.reactive.function.server.ServerRequest;

/**
 * <p>Title: ServerRequestSpanTextMap</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.ServerRequestSpanTextMap</code></p>
 */

public class ServerRequestSpanTextMap implements SpanTextMap {
	private final ServerRequest delegate;
	
	static final String URI_HEADER = "X-Span-Uri";

	ServerRequestSpanTextMap(ServerRequest delegate) {
		this.delegate = delegate;
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.cloud.sleuth.SpanTextMap#iterator()
	 */
	@Override
	public Iterator<Entry<String, String>> iterator() {
		HashMap<String, String> map = new HashMap<>(delegate.headers().asHttpHeaders().toSingleValueMap());
		map.put(URI_HEADER, delegate.uri().toString());
		return map.entrySet().iterator();
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.cloud.sleuth.SpanTextMap#put(java.lang.String, java.lang.String)
	 */
	@Override
	public void put(String key, String value) {
		delegate.headers().asHttpHeaders().set(key, value);
	}

}
