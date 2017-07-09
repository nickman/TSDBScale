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
package com.heliosapm.tsdbscale.core.repositories;

import java.util.Date;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanAccessor;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.stereotype.Component;

import com.heliosapm.tsdbscale.core.namespace.NamespaceEntity;

import reactor.core.publisher.Mono;


/**
 * <p>Title: NamespaceEntityRepository</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.repositories.NamespaceEntityRepository</code></p>
 */
@Component
public class OldNamespaceRepository extends SimpleReactiveMongoRepository<NamespaceEntity, ObjectId> {
	private static final Logger LOG = LoggerFactory.getLogger(OldNamespaceRepository.class);
	
	
	
	public OldNamespaceRepository(@Autowired @Qualifier("ns") ReactiveMongoDatabaseFactory mongoDbFactory) {
		super(new MongoEntityInformation<NamespaceEntity, ObjectId>(){

			@Override
			public boolean isNew(NamespaceEntity entity) {
				return entity.isNewId();
			}

			@Override
			public Optional<ObjectId> getId(NamespaceEntity entity) {
				return Optional.ofNullable(entity.getId());
			}

			@Override
			public Class<ObjectId> getIdType() {
				return ObjectId.class;
			}

			@Override
			public Class<NamespaceEntity> getJavaType() {
				return NamespaceEntity.class;
			}

			@Override
			public String getCollectionName() {
				return "NamespaceEntries";
			}

			@Override
			public String getIdAttribute() {
				return "id";
			}
			
		}, new ReactiveMongoTemplate(mongoDbFactory));
		LOG.info("-----------> Created NamespaceEntity Repo");
		
	}

	public Mono<NamespaceEntity> findById(String id) {
		return findById(new ObjectId(id));
	}
}
