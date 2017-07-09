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
package com.heliosapm.tsdbscale.core.namespace;

import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.publisher.Mono;

/**
 * <p>Title: NamespaceConverter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.namespace.NamespaceConverter</code></p>
 */

public class NamespaceConverter {


	public static Namespace convert(NamespaceEntity source) {
		return new Namespace()
				.withId(source.getId().toString())
				.withName(source.getName())
				.withVersion(source.getVersion())
				.withWebsiteId(source.getWebsiteId().toString())
				.withResourceId(String.valueOf(source.getResourceId()))
				.withCreatedAt(source.getCreatedAt())
				.withUpdatedAt(source.getUpdatedAt());
	}
	
	public static Mono<Namespace> convertMono(Mono<NamespaceEntity> source) {
		return Mono.from(source)
			.flatMap(ne -> {
				return Mono.just(convert(ne));
			});
	}

	private NamespaceConverter() {

	}


}
