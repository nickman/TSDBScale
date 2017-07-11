/**
 * 
 */
package com.heliosapm.tsdbscale.core.handlers;

import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;
import com.heliosapm.tsdbscale.core.repositories.TSDBMetricRepository;
import com.heliosapm.tsdbscale.util.JSONOps;

import reactor.core.publisher.Mono;

/**
 * @author nwhitehead
 *
 */
@Component
@RestController
public class TSDBMetricHandler {

	private static final Logger LOG = LoggerFactory.getLogger(TSDBMetricHandler.class);
	
	@Autowired
	protected TSDBMetricRepository repo = null;

	
	public Mono<ServerResponse> echo(ServerRequest request) {
		return ServerResponse.ok().body(request.bodyToMono(String.class), String.class);
	}
	
//	@GetMapping("/resolve/{expression}")
	public Mono<ServerResponse> resolveGet(ServerRequest request) {
		String foo = request.pathVariable("foo");
		final Set<TSDBMetric> set = repo.resolveMetrics(foo).toStream().collect(Collectors.toSet());
		return ServerResponse.ok().syncBody(JSONOps.serializeToString(set.toArray(new TSDBMetric[set.size()])));
	}
	
	public Mono<ServerResponse> resolvePut(ServerRequest request) {
		return ServerResponse.ok().body(repo.resolveMetrics(request.bodyToMono(String.class)), TSDBMetric.class);		
	}
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
