/**
 * 
 */
package com.heliosapm.tsdbscale.core.repositories;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.pgasync.Db;
import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;
import com.heliosapm.tsdbscale.reactor.ReactorTrace;
import com.heliosapm.tsdbscale.util.JSONOps;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;
import rx.RxReactiveStreams;

/**
 * @author nwhitehead
 *
 */
@Repository("TSDBScaleRepository")
public class TSDBMetricRepositoryImpl implements TSDBMetricRepository {
	private static final Logger LOG = LoggerFactory.getLogger(TSDBMetricRepositoryImpl.class);
	@Autowired
	protected Db db = null;
	@Autowired 
	protected ReactorTrace rtracer;
	
	public TSDBMetricRepositoryImpl() {
		LOG.info("Built TSDBMetricRepositoryImpl");
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbscale.core.repositories.TSDBMetricRepository#resolveMetrics(java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Flux<TSDBMetric> resolveMetrics(String expression) {
//		final Span parent = tracer.getCurrentSpan();
//		final Span span = tracer.createSpan("resolveMetricsX", parent);
//		span.tag("lc", "TSDBMetricRepository");
//		tracer.detach(span);
//		LOG.info("Resolving expression [{}]", expression);
//		final AtomicBoolean first = new AtomicBoolean(false);
//		final long start = System.nanoTime();
//		final long[] tt = new long[2];
//		final int[] count = new int[]{0};
//		final Observable<TSDBMetric> ob = db.queryRows("select * from putMetrics(jsonb($1::text))", expression)
//		.map(r -> {
//			count[0]++;
//			if(first.compareAndSet(false, true)) {
//				tt[0] = System.nanoTime() - start;				
//			}
//			return r.get(0, ObjectNode.class);
//		})
//		.map(o -> {
//			tt[1] = System.nanoTime() - start;
//			
//			tracer.continueSpan(span).tag("ttfr", "" + TimeUnit.NANOSECONDS.toMicros(tt[0]));
//			tracer.continueSpan(span).tag("ttlr", "" + TimeUnit.NANOSECONDS.toMicros(tt[1]));
//			tracer.continueSpan(span).tag("ttir", "" + TimeUnit.NANOSECONDS.toMicros(tt[1]-tt[0]));
//			tracer.continueSpan(span).tag("rows", "" + count[0]);
//			return JSONOps.parseToObject(o, TSDBMetric.class);
//		});
//		
		
		final Observable<TSDBMetric> ob = db.queryRows("select * from putMetrics(jsonb($1::text))", expression)
			.map(r -> JSONOps.parseToObject(r.get(0, ObjectNode.class), TSDBMetric.class));
		final Flux<TSDBMetric> traced = Flux.concat(RxReactiveStreams.toPublisher(ob));
		return rtracer.trace(traced, "resolveMetrics", "TSDBMetricRepository");
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbscale.core.repositories.TSDBMetricRepository#resolveMetrics(java.lang.String)
	 */
	@Override
	public Flux<TSDBMetric> resolveMetricsFast(String expression) {
		final Observable<TSDBMetric> ob =
				db.queryRows("select * from putMetrics(jsonb($1::text))", expression)
				.map(r -> r.get(0, ObjectNode.class))				
				.map(o -> JSONOps.parseToObject(o, TSDBMetric.class));
				
		return Flux.concat(RxReactiveStreams.toPublisher(ob));
	}
	
	// ==== HERE
	public Flux<TSDBMetric> resolveMetrics(Mono<String> expression) {		
		return expression.flatMapMany(t -> {
			Observable<TSDBMetric> ob = db.queryRows("select * from putMetrics(jsonb($1::text))", t)
					.map(r -> r.get(0, ObjectNode.class))
					.map(o -> JSONOps.parseToObject(o, TSDBMetric.class));	
			LOG.info("Created TSDBMetric observer: {} for expression: {}", ob, t);
			Flux<TSDBMetric> traced = Flux.concat(RxReactiveStreams.toPublisher(ob));	
			return rtracer.trace(traced, "resolveMetrics", "TSDBMetricRepository");
		});					
	}
	
	@SuppressWarnings("unchecked")
	public Mono<TSDBMetric> getMetric(String expression) {
		final Observable<TSDBMetric> ob = db.queryRows("select * from putMetrics(jsonb($1::text))", expression)
				.map(r -> r.get(0, ObjectNode.class))
				.map(o -> JSONOps.parseToObject(o, TSDBMetric.class));			
		
		final Mono<TSDBMetric> traced = Mono.fromDirect(RxReactiveStreams.toPublisher(ob.toSingle())); 
		return rtracer.trace(traced, "getMetric/" + expression, "TSDBMetricRepository");
		//return traced;
	}
	
	public Mono<TSDBMetric> getMetric(Mono<String> expression) {
		return expression.flatMap(s -> getMetric(s));
	}
	
	public Flux<TSDBMetric> resolveMetrics2(Mono<String> expression) {		
		return expression.flatMapMany(new Function<String, Publisher<TSDBMetric>>() {

			@Override
			public Publisher<TSDBMetric> apply(String t) {
				Observable<TSDBMetric> ob = db.queryRows("select * from putMetrics(jsonb($1::text))", t)
						.map(r -> r.get(0, ObjectNode.class))
						.map(o -> JSONOps.parseToObject(o, TSDBMetric.class));				
				return RxReactiveStreams.toPublisher(ob);
				
			}
			
		});
	}
	
//	public Flux<TSDBMetric> resolveMetrics3(final Mono<String> expression) {
//		try {
//			return tracer.wrap(new Callable<Flux<TSDBMetric>>(){
//				@Override
//				public Flux<TSDBMetric> call() throws Exception {
//					return expression.flatMapMany(t -> {
//						Observable<TSDBMetric> ob = db.queryRows("select * from putMetrics(jsonb($1::text))", t)
//								.map(r -> r.get(0, ObjectNode.class))
//								.map(o -> JSONOps.parseToObject(o, TSDBMetric.class));	
//						LOG.info("Created TSDBMetric observer: {} for expression: {}", ob, t);
//						return RxReactiveStreams.toPublisher(ob);			
//					});					
//				}
//			}).call();
//		} catch (Exception ex) {
//			LOG.error("Failed to execute resolveMetrics3", ex);
//			throw new RuntimeException(ex);
//		}
//	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbscale.core.repositories.TSDBMetricRepository#getMetric(long)
	 */
	@Override
	public Mono<TSDBMetric> getMetric(long metricId) {
		// TODO Auto-generated method stub
		return null;
	}

}
