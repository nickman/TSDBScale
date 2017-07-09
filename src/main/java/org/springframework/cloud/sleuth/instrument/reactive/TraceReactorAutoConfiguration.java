/**
 * 
 */
package org.springframework.cloud.sleuth.instrument.reactive;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.sleuth.SpanNamer;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.autoconfig.TraceAutoConfiguration;
import org.springframework.cloud.sleuth.instrument.async.TraceableScheduledExecutorService;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

/**
 * @author nwhitehead
 *
 */
@Configuration
//@ConditionalOnProperty(value="spring.sleuth.reactor.enabled", matchIfMissing=true)
@ConditionalOnClass(Mono.class)
@AutoConfigureAfter(TraceAutoConfiguration.class)
public class TraceReactorAutoConfiguration {

	@Configuration
	@ConditionalOnBean(Tracer.class)
	static class TraceReactorConfiguration {
		@Autowired Tracer tracer;
		@Autowired TraceKeys traceKeys;
		@Autowired SpanNamer spanNamer;

		@PostConstruct
		public void setupHooks() {
			Hooks.onNewSubscriber((pub, sub) ->
					new SpanSubscriber(sub, Context.from(sub), this.tracer, pub.toString()));
			Schedulers.setFactory(new Schedulers.Factory() {
				@Override public ScheduledExecutorService decorateScheduledExecutorService(
						String schedulerType,
						Supplier<? extends ScheduledExecutorService> actual) {
					return new TraceableScheduledExecutorService(actual.get(),
							TraceReactorConfiguration.this.tracer,
							TraceReactorConfiguration.this.traceKeys,
							TraceReactorConfiguration.this.spanNamer);
				}
			});
		}
	}
}
