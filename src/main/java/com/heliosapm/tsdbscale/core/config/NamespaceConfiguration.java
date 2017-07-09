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
package com.heliosapm.tsdbscale.core.config;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanAccessor;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.stereotype.Component;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * <p>Title: NamespaceConfiguration</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.config.NamespaceConfiguration</code></p>
 */
@EnableReactiveMongoRepositories
@Component
public class NamespaceConfiguration extends AbstractReactiveMongoConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(NamespaceConfiguration.class);
	@Autowired 
	protected Tracer tracer;
	@Autowired
	private SpanAccessor accessor;	


//	@Bean
//	public AbstractMongoEventListener<MongoMappingEvent<?>> mongoEventListener() {
//		return new AbstractMongoEventListener<MongoMappingEvent<?>>() {
//			@Override
//			public void onAfterLoad(AfterLoadEvent<MongoMappingEvent<?>> event) {
//				Span span = accessor.getCurrentSpan();
//				span.tag("collection", event.getCollectionName());
//				span.tag("collection", "" + event.getTimestamp());
//				span.tag("source", event.getSource().toString());
//				super.onAfterLoad(event);
//			}
//		};
//	}

	@Override
	@Bean
	public MongoClient mongoClient() {
		return MongoClients.create(String.format("mongodb://localhost:%d", 27017));
	}

	@Override
	protected String getDatabaseName() {
		return "SquarespaceNS";
	}

	@Bean("ns")
	@Primary
	public ReactiveMongoDatabaseFactory mongoDatabaseFactory(final MongoClient client) {
		return new SimpleReactiveMongoDatabaseFactory(client, getDatabaseName());
	}


}
