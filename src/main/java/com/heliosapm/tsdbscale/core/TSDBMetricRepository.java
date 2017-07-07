/**
 * 
 */
package com.heliosapm.tsdbscale.core;

import com.heliosapm.tsdbscale.core.metrics.*;
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
	public Flux<TSDBMetric> resolveMetrics3(Mono<String> expression);
	
	
}
