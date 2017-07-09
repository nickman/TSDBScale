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
package com.heliosapm.tsdbscale.core.handlers;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanAccessor;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.instrument.reactive.TracePublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;

/**
 * <p>Title: WelcomeController</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.handlers.WelcomeController</code></p>
 */

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;
import com.heliosapm.tsdbscale.core.namespace.Namespace;
import com.heliosapm.tsdbscale.core.namespace.NamespaceConverter;
import com.heliosapm.tsdbscale.core.repositories.ReactiveNamespaceRepository;
import com.heliosapm.tsdbscale.core.repositories.TSDBMetricRepository;
import com.heliosapm.tsdbscale.util.JSONOps;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class WelcomeController {
	
	private static final Logger LOG = LoggerFactory.getLogger(WelcomeController.class);
	@Autowired 
	protected Tracer tracer;
	@Autowired
	private SpanAccessor accessor;	
	@Autowired 
	TraceKeys traceKeys;
	
	@Autowired
	protected TSDBMetricRepository repo = null;
	//@Autowired SimpleReactiveMongoRepository<TSDBMetric, Long> mongoRepo = null;
	@Autowired
	protected ReactiveNamespaceRepository nsRepo = null;
	//protected OldNamespaceRepository nsRepo = null;
	@GetMapping("/")
	public String welcome() {
		return "Hello World";
	}
	
	private final ThreadLocal<Span> tspan = new ThreadLocal<Span>();
	
	 
	
	
//	@Bean
//	public LoggingEventListener mongoEventListener() {
//		return new LoggingEventListener() {
//			@Override
//			public void onAfterLoad(AfterLoadEvent<Object> event) {
//				
//				final boolean newSpan;
//				Span span = tracer.getCurrentSpan();
//				if(span==null) {
//					newSpan = true;
//					span = tracer.createSpan("onAfterLoad");
//				} else {
//					newSpan = false;
//				}
//				span.tag("thread", Thread.currentThread().getName());
//				
//				span.tag("collection", event.getCollectionName());
//				span.tag("timestamp", new Date(event.getTimestamp()).toString());
//				span.tag("source", event.getSource().toString());
//				if(newSpan) tracer.close(span);
//				super.onAfterLoad(event);
//			}
//		};
//	}
	
	private Span span(String spanName, String component) {
		final Span parent = tracer.getCurrentSpan();
		final Span span = tracer.createSpan(spanName, parent);
		span.tag("lc", component);
		//tracer.detach(span);
		tspan.set(span);
		return span;
	}

	@RequestMapping(path = "/namespace/{name}", method = RequestMethod.GET, produces = "application/json; charset=UTF-8")
	public Mono<Namespace> namespaceByName(@PathVariable(required=true) final String name) {
		final Span span = tracer.createSpan("mongo-namespaceByName", tracer.getCurrentSpan()); 
		tracer.detach(span);
		
		Mono<Namespace> traced = nsRepo.findByName(name)
				.transform(ne -> {
					LOG.info("In Thread: {}", Thread.currentThread());
					Mono<Namespace> m = NamespaceConverter.convertMono(ne).doFinally(sig -> {
						Span sp = tracer.continueSpan(span);
						sp.tag("lc", "mongo-namespaceByName-" + sig.name());
						sp.tag("signal", sig.name());
						sp.tag("thread", Thread.currentThread().getName());
						tracer.close(sp);
//						tspan.remove();
					});			
					return m; 
				});
		
		return traced; //Mono.from(TracePublisher.from(traced,  tracer, traceKeys));
	}

	
	
	
	@GetMapping("/resolve/{expression}")
	public String resolveGet(@PathVariable("expression") final String expression) {
		final Set<TSDBMetric> set = repo.resolveMetrics(expression).toStream().collect(Collectors.toSet());
		return JSONOps.serializeToString(set.toArray(new TSDBMetric[set.size()]));
	}
	
	@PostMapping("/resolve")
	public String resolvePut(@RequestBody final String expression) {
		
		final Set<TSDBMetric> set = repo.resolveMetrics(expression).toStream().collect(Collectors.toSet());
		return JSONOps.serializeToString(set.toArray(new TSDBMetric[set.size()]));
	}
	
	@PostMapping("/async/resolve")
	public Flux<TSDBMetric> asyncResolvePut(@RequestBody final String expression) {
		return repo.resolveMetrics(expression);
		
	}
	
	@PostMapping("/async/resolvefast")
	public Flux<TSDBMetric> asyncResolvePutFast(@RequestBody final String expression) {
		return repo.resolveMetricsFast(expression);
	}
	
	
	
	@RequestMapping(path = "/namespacebyw/{objectId}", method = RequestMethod.GET, produces = "application/json; charset=UTF-8")
	public Mono<Namespace> namespaceByWebsite(@PathVariable(required=true) final String objectId) {
		final Span parent = tracer.getCurrentSpan();
		final Span span = tracer.createSpan("mongo-namespaceByWebsite", parent);
		span.tag("lc", "MongoDb-Namespace");
		tracer.detach(span);
		
		return nsRepo.findById(new ObjectId(objectId)).transform(ne -> {
			return NamespaceConverter.convertMono(ne).doFinally(sig -> {
				Span sp = tracer.continueSpan(span);
				sp.tag("signal", sig.name());
				tracer.close(sp);
			});
		});
	}
	


}