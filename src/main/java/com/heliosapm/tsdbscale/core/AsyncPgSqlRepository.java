/**
 * 
 */
package com.heliosapm.tsdbscale.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.pgasync.Db;
import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;
import com.heliosapm.tsdbscale.util.JSONOps;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;
import rx.RxReactiveStreams;

/**
 * @author nwhitehead
 *
 */
public class AsyncPgSqlRepository {

	private static final Logger LOG = LoggerFactory.getLogger(AsyncPgSqlRepository.class);
	@Autowired
	protected Db db = null;
	
//	public <T> Flux<T> query

	
//	public Flux<TSDBMetric> resolveMetrics3(Mono<String> expression) {		
//		return expression.flatMapMany(t -> {
//			Observable<TSDBMetric> ob = db.queryRows("select * from putMetrics(jsonb($1::text))", t)
//					.map(r -> r.get(0, ObjectNode.class))
//					.map(o -> JSONOps.parseToObject(o, TSDBMetric.class));	
//			LOG.info("Created TSDBMetric observer: {} for expression: {}", ob, t);
//			return RxReactiveStreams.toPublisher(ob);			
//		});
//	}

	
	
}
