/**
 * 
 */
package com.heliosapm.tsdbscale.core.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.reactive.function.server.ServerRequest;

import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;
import com.heliosapm.tsdbscale.core.repositories.TSDBMetricRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author nwhitehead
 *
 */
//@RestController
//@Component
@RequestMapping("/metrics")
public class TSDBMetricHandler {

	private static final Logger LOG = LoggerFactory.getLogger(TSDBMetricHandler.class);
	
	@Autowired
	protected TSDBMetricRepository repo = null;
	@Autowired
	protected Tracer tracer = null;
	
//	@GetMapping(path = "/one/{expression}")
    public Mono<TSDBMetric> get(@PathVariable("expression") String expression) {
        return this.repo.getMetric(expression);
    }	

    public Flux<TSDBMetric> stream(ServerRequest request) {
    	return repo.resolveMetrics(request.bodyToMono(String.class));
    }
	
//	public Mono<ServerResponse> echo(ServerRequest request) {
//		return ServerResponse.ok().body(request.bodyToMono(String.class), String.class);
//	}
//	
//	
//	public Mono<ServerResponse> resolveGet(ServerRequest request) {
//		String expression = request.pathVariable("expression");
//		final Set<TSDBMetric> set = repo.resolveMetrics(expression).toStream().collect(Collectors.toSet());
//		return ServerResponse.ok().syncBody(JSONOps.serializeToString(set.toArray(new TSDBMetric[set.size()])));
//	}
//	
//	
//	public Mono<ServerResponse> resolvePut(ServerRequest request) {
//		return ServerResponse.ok().body(repo.resolveMetrics(request.bodyToMono(String.class)), TSDBMetric.class);		
//	}
//	
//	@PostMapping("/resolve")
//	public String resolvePut(@RequestBody final String expression) {
//		final Set<TSDBMetric> set = repo.resolveMetrics(expression).toStream().collect(Collectors.toSet());
//		return JSONOps.serializeToString(set.toArray(new TSDBMetric[set.size()]));
//	}
//	
//	@PostMapping("/async/resolve")
//	public Flux<TSDBMetric> asyncResolvePut(@RequestBody final String expression) {
//		return repo.resolveMetrics(expression);
//		
//	}
//		

}
