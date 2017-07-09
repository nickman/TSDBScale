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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.stereotype.Component;

import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;

/**
 * <p>Title: TSDBMetricMongoRepository</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.repositories.TSDBMetricMongoRepository</code></p>
 */
@Component
public class TSDBMetricMongoRepository extends SimpleReactiveMongoRepository<TSDBMetric, Long> {
	private static final Logger LOG = LoggerFactory.getLogger(TSDBMetricMongoRepository.class);
	
	public TSDBMetricMongoRepository(@Autowired ReactiveMongoOperations mongoOperations) {
		super(new MongoEntityInformation<TSDBMetric, Long>(){

			@Override
			public boolean isNew(TSDBMetric entity) {
				return entity.getMetricId()==-1;
			}

			@Override
			public Optional<Long> getId(TSDBMetric entity) {
				return Optional.ofNullable(entity.getMetricId()==-1 ? null : entity.getMetricId());
			}

			@Override
			public Class<Long> getIdType() {
				return Long.class;
			}

			@Override
			public Class<TSDBMetric> getJavaType() {
				return TSDBMetric.class;
			}

			@Override
			public String getCollectionName() {
				return "metrics";
			}

			@Override
			public String getIdAttribute() {
				return "id";
			}
			
		}, mongoOperations);
		LOG.info("-----------> Created Mongo Repo");
	}



}
