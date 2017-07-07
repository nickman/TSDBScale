package com.heliosapm.tsdbscale.core;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.http.MediaType.TEXT_PLAIN;
import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;
import static org.springframework.web.reactive.function.server.RequestPredicates.contentType;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.hystrix.EnableHystrix;
import org.springframework.cloud.netflix.hystrix.dashboard.EnableHystrixDashboard;
import org.springframework.cloud.sleuth.SpanNamer;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.instrument.async.TraceableScheduledExecutorService;
import org.springframework.cloud.sleuth.reactor.SpanSubscriber;
import org.springframework.cloud.sleuth.reactor.TraceReactorAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import com.heliosapm.tsdbscale.core.config.PGConfiguration;
import com.heliosapm.tsdbscale.core.converters.Converters.JsonArrayConverter;
import com.heliosapm.tsdbscale.core.converters.Converters.JsonNodeConverter;
import com.heliosapm.tsdbscale.core.converters.Converters.JsonObjectConverter;
import com.heliosapm.tsdbscale.core.converters.Converters.NumberArrayConverter;
import com.heliosapm.tsdbscale.core.converters.Converters.TSDBMetricConverter;
import com.heliosapm.tsdbscale.core.handlers.EchoHandler;
import com.heliosapm.tsdbscale.core.handlers.TSDBMetricHandler;

import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.util.context.Context;


@SpringBootApplication
@EnableRetry
@EnableDiscoveryClient
@EnableAutoConfiguration
@EnableHystrix
@EnableHystrixDashboard
@Configuration
@Import({
	PGConfiguration.class,
	TraceReactorAutoConfiguration.class
})

public class CoreApplication implements InitializingBean {
	
	private static final Logger LOG = LoggerFactory.getLogger(CoreApplication.class);
	
	@Autowired Tracer tracer;
	@Autowired TraceKeys traceKeys;
	@Autowired SpanNamer spanNamer;
	

	public static void main(String[] args) {
		System.setProperty("spring.application.name", "tsdb-scale");
		System.setProperty("spring.sleuth.reactor.enabled", "true");
		System.setProperty("spring.sleuth.enabled", "true");
		
		
		SpringApplication.run(CoreApplication.class, args);
	}
	

	@Bean
	public RouterFunction<ServerResponse> monoRouterFunction(EchoHandler echoHandler, TSDBMetricHandler metricHandler) {
		return route(GET("/echo").and(accept(TEXT_PLAIN)), echoHandler::echo)
		.andRoute(POST("/echo").and(contentType(TEXT_PLAIN)), echoHandler::echo)
		.andRoute(GET("/metrics"), metricHandler::resolveGet)
		.andRoute(POST("/metrics").and(contentType(APPLICATION_JSON)), metricHandler::resolvePut);
	}	
	
	@Bean(destroyMethod="close")
	public Db pgConnPool(PGConfiguration config) {
		Db db = new ConnectionPoolBuilder()
		.hostname(config.getHost())
		.port(config.getPort())
		.database(config.getDatabase())
		.username(config.getUser())
		.password(config.getPassword())
		.poolSize(config.getPoolSize())
		.converters(new JsonNodeConverter(), new JsonArrayConverter(), new NumberArrayConverter(), new TSDBMetricConverter(), new JsonObjectConverter())
		.build();
		LOG.info("Provisioned AsyncPG Connection Pool: {}@{}:{}, pool size: {}", config.getUser(), config.getHost(), config.getPort(), config.getPoolSize());
		return db;
		
	}
	
	@Bean("HttpServer")
	public HttpServer httpServer(RouterFunction<ServerResponse> routerFunction) {
		HttpServer server = HttpServer.create(opt -> opt 
			.listen("0.0.0.0", 8081)
			.preferNative(true)			
		);
		LOG.info("HttpServer: {}", server);
		return server;
	}
	
	@Bean(name="NettyContext", destroyMethod="dispose")
	public NettyContext nettyContext(HttpServer server, RouterFunction<ServerResponse> routerFunction) {
		HttpHandler httpHandler = RouterFunctions.toHttpHandler(routerFunction);
		ReactorHttpHandlerAdapter adapter = new ReactorHttpHandlerAdapter(httpHandler);
		NettyContext ctx = server.newHandler(adapter).block();
		LOG.info("NettyContext: {}", ctx);		
		return ctx;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		setupHooks();
		
	}
	
	public void setupHooks() {
		Hooks.onNewSubscriber((pub, sub) ->
				new SpanSubscriber(sub, Context.from(sub), this.tracer, pub.toString()));
		Schedulers.setFactory(new Schedulers.Factory() {
			@Override public ScheduledExecutorService decorateScheduledExecutorService(
					String schedulerType,
					Supplier<? extends ScheduledExecutorService> actual) {
				return new TraceableScheduledExecutorService(actual.get(),
						tracer,
						traceKeys,
						spanNamer);
			}
		});
	}
	
}



