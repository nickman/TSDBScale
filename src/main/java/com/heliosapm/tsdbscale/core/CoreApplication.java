package com.heliosapm.tsdbscale.core;

import static org.springframework.http.MediaType.TEXT_PLAIN;
import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;
import static org.springframework.web.reactive.function.server.RequestPredicates.contentType;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.hystrix.EnableHystrix;
import org.springframework.cloud.netflix.hystrix.dashboard.EnableHystrixDashboard;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.heliosapm.tsdbscale.core.handlers.EchoHandler;

@SpringBootApplication
@EnableRetry
@EnableDiscoveryClient
@EnableAutoConfiguration
@EnableHystrix
@EnableHystrixDashboard
public class CoreApplication {

	public static void main(String[] args) {
		SpringApplication.run(CoreApplication.class, args);
	}
	

	@Bean
	public RouterFunction<ServerResponse> monoRouterFunction(EchoHandler echoHandler) {
		return route(GET("/echo").and(accept(TEXT_PLAIN)), echoHandler::echo)
		.andRoute(POST("/echo").and(contentType(TEXT_PLAIN)), echoHandler::echo);		
	}	
}
