/**
 * 
 */
package com.heliosapm.tsdbscale.core.repositories;

import org.springframework.cloud.sleuth.SpanName;
import org.springframework.cloud.sleuth.annotation.SpanTag;
import org.springframework.scheduling.annotation.Async;

import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
/**
 * @author nwhitehead
 *
 */
public interface TSDBMetricRepository {

	public Flux<TSDBMetric> resolveMetrics(String expression);
	public Mono<TSDBMetric> getMetric(long metricId);

	public Flux<TSDBMetric> resolveMetrics(Mono<String> expression);
	@Async
	public Flux<TSDBMetric> resolveMetricsFast(String expression);
	
	
}
