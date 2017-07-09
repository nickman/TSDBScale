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

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Tailable;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.heliosapm.tsdbscale.core.namespace.NamespaceEntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * <p>Title: ReactiveNamespaceRepository</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.repositories.ReactiveNamespaceRepository</code></p>
 */

public interface ReactiveNamespaceRepository extends ReactiveCrudRepository<NamespaceEntity, ObjectId> {
	
	@Query("{ 'name': ?0}")
	Mono<NamespaceEntity> findByName(Mono<String> name);
	
	@Query("{ 'name': ?0}")
	Mono<NamespaceEntity> findByName(String name);
	
	
	@Query("{ 'websiteId': ?0}")
	Mono<NamespaceEntity> findByWebsiteId(Mono<ObjectId> websiteId);
	
	@Query("{ 'websiteId': ?0}")
	Mono<NamespaceEntity> findByWebsiteId(ObjectId websiteId);
	
	
	@Tailable
	Flux<NamespaceEntity> findWithTailableCursorBy();
}
