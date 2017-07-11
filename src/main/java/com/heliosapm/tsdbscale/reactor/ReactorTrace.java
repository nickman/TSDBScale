/**
 * 
 */
package com.heliosapm.tsdbscale.reactor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Sampler;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author nwhitehead
 *
 */
@Component
public class ReactorTrace implements Tracer {
	
	/** The tracing tag name for stringified exceptions */
	public static final String ERROR_TAG_NAME = "err";
	/** The tracing tag name for the completion signal */
	public static final String SIGNAL_TAG_NAME = "sig";
	/** The tracing tag name for (flux) time to first response */
	public static final String TIME_TO_FIRST_TAG_NAME = "ttf";
	/** The tracing tag name for (flux) time to last response */
	public static final String TIME_TO_LAST_TAG_NAME = "ttl";
	/** The tracing tag name for (flux) time to iterate responses (ttl - ttf) */
	public static final String TIME_TO_ITERATE_TAG_NAME = "tti";
	/** The tracing tag name for the (flux)  number of responses */
	public static final String RESPONSE_COUNT_TAG_NAME = "rc";
	
	public static final int FLUX_TIME_START = 0;
	public static final int FLUX_TIME_FIRST = 1;
	public static final int FLUX_TIME_LAST = 2;
	
	
	
	private static final Logger LOG = LoggerFactory.getLogger(ReactorTrace.class);
	@Autowired 
	protected Tracer tracer;
	@Autowired 
	TraceKeys traceKeys;
	
	
	/**
	 * Adds span tracing to the completion of the target mono
	 * @param target The target mono
	 * @param spanName The span name
	 * @param componentName The span component name
	 * @param traceCustomizers An optional array of span customizer
	 * @return the resulting mono
	 */
	
	public <T> Mono<T> trace(final Mono<T> target, final String spanName, final String componentName, @SuppressWarnings("unchecked") BiConsumer<Tracer, TraceKeys>...traceCustomizers) {
		if(!tracer.isTracing()) return target;
		final Span[] span = new Span[1];
		
		return target.doOnSubscribe(s -> {
			if(LOG.isDebugEnabled()) LOG.debug("Enabled mono trace: component={}, span={}", spanName, componentName);
			final Span sp = tracer.createSpan(spanName, tracer.getCurrentSpan());
			tracer.detach(sp);
			span[0] = sp;
		})
		.doOnError(t -> {
			Span tspan = tracer.continueSpan(span[0]);
			tspan.tag(ERROR_TAG_NAME, t.toString());
			tracer.detach(tspan);
			LOG.error("Caught exception in mono: component={}, span={}", spanName, componentName, t);
		})			
		.doOnSuccess(t -> {
			if(LOG.isInfoEnabled()) LOG.info("Mono completed: component={}, span={}", spanName, componentName);
		})
		.doFinally(sig -> {
			Span tspan = tracer.continueSpan(span[0]);
			this.tracer.addTag(SIGNAL_TAG_NAME, sig.name());
			this.tracer.addTag(this.traceKeys.getAsync().getPrefix()
					+ this.traceKeys.getAsync().getThreadNameKey(), Thread.currentThread().getName());
			if(traceCustomizers!=null) {
				for(BiConsumer<Tracer, TraceKeys> custom: traceCustomizers) {
					if(custom!=null) {
						custom.accept(tracer, traceKeys);
					}
				}
			}
			if (!tspan.tags().containsKey(Span.SPAN_LOCAL_COMPONENT_TAG_NAME)) {
				this.tracer.addTag(Span.SPAN_LOCAL_COMPONENT_TAG_NAME, componentName);
			}
			tracer.close(tspan);						
		});
	}
	
	
	/**
	 * Adds span tracing to the completion of the target flux
	 * @param target The target flux
	 * @param spanName The span name
	 * @param componentName The span component name
	 * @param traceCustomizers An optional array of span customizer
	 * @return the resulting flux
	 */
	
	public <T> Flux<T> trace(final Flux<T> target, final String spanName, final String componentName, @SuppressWarnings("unchecked") BiConsumer<Tracer, TraceKeys>...traceCustomizers) {
		if(!tracer.isTracing()) return target;
		final Span[] span = new Span[1];
		final Span[] iteratingSpan = new Span[1];
		final long[] times = new long[3];  // start time, ttf, ttl
		final int[] count = new int[1];
		final AtomicBoolean first = new AtomicBoolean(false);
		this.tracer.addTag(this.traceKeys.getAsync().getPrefix()
				+ this.traceKeys.getAsync().getThreadNameKey(), Thread.currentThread().getName());
		final Set<Long> nextThreadNames = new HashSet<Long>(24);
		return target.doOnSubscribe(s -> {
			times[FLUX_TIME_START] = System.nanoTime();
			if(LOG.isDebugEnabled()) LOG.debug("Enabled flux trace: component={}, span={}", spanName, componentName);
			Span sp = tracer.createSpan(spanName, tracer.getCurrentSpan());
			tracer.detach(sp);
			span[0] = sp;
			
		})
		.doOnNext(sig -> {
			count[0]++;
			//LOG.info("-----On each: count={}, sig={}, sigType={}", count[0], sig.n, sig.getClass().getName());
			nextThreadNames.add(Thread.currentThread().getId());
			if(first.compareAndSet(false, true)) {
				times[FLUX_TIME_FIRST] = System.nanoTime() - times[FLUX_TIME_START];				
				Span isp = tracer.createSpan(spanName + "-iteration", span[0]);
				tracer.detach(isp);
				iteratingSpan[0] = isp;
			}
		})
		.doOnError(t -> {
			Span tspan = tracer.continueSpan(span[0]);
			tspan.tag(ERROR_TAG_NAME, t.toString());
			tracer.detach(tspan);
			LOG.error("Caught exception in flux: component={}, span={}", spanName, componentName, t);
		})			
		.doOnComplete( new Runnable() {
			public void run() {
				Span tspan = tracer.continueSpan(iteratingSpan[0]);
				times[FLUX_TIME_LAST] = System.nanoTime() - times[FLUX_TIME_START];
				if(LOG.isInfoEnabled()) LOG.info("Flux completed: component={}, span={}", spanName, componentName);
				ReactorTrace.this.tracer.addTag(ReactorTrace.this.traceKeys.getAsync().getPrefix()
						+ ReactorTrace.this.traceKeys.getAsync().getThreadNameKey(), Thread.currentThread().getName());
				ReactorTrace.this.tracer.addTag("tc", "" + nextThreadNames.size());
				
				if (!tspan.tags().containsKey(Span.SPAN_LOCAL_COMPONENT_TAG_NAME)) {
					ReactorTrace.this.tracer.addTag(Span.SPAN_LOCAL_COMPONENT_TAG_NAME, componentName);
				}
				ReactorTrace.this.tracer.close(tspan);						
			}
		})
		.doFinally(sig -> {
			Span tspan = tracer.continueSpan(span[0]);
			if(times[FLUX_TIME_FIRST]!=0L && times[FLUX_TIME_LAST]!=0L) {
				final long ttf = TimeUnit.NANOSECONDS.toMicros(times[FLUX_TIME_FIRST]);
				final long ttl = TimeUnit.NANOSECONDS.toMicros(times[FLUX_TIME_LAST]);
				final long tti = ttl - ttf;
				this.tracer.addTag(TIME_TO_FIRST_TAG_NAME, "" + ttf);
				this.tracer.addTag(TIME_TO_LAST_TAG_NAME, "" + ttl);
				this.tracer.addTag(TIME_TO_ITERATE_TAG_NAME, "" + tti);
			}
			this.tracer.addTag(RESPONSE_COUNT_TAG_NAME, "" + count[0]);
				
			
			this.tracer.addTag(SIGNAL_TAG_NAME, sig.name());
			this.tracer.addTag(this.traceKeys.getAsync().getPrefix()
					+ this.traceKeys.getAsync().getThreadNameKey(), Thread.currentThread().getName());
			if(traceCustomizers!=null) {
				for(BiConsumer<Tracer, TraceKeys> custom: traceCustomizers) {
					if(custom!=null) {
						custom.accept(tracer, traceKeys);
					}
				}
			}
			if (!tspan.tags().containsKey(Span.SPAN_LOCAL_COMPONENT_TAG_NAME)) {
				this.tracer.addTag(Span.SPAN_LOCAL_COMPONENT_TAG_NAME, componentName);
			}
			tracer.close(tspan);						
		});
	}


	/**
	 * @return
	 * @see org.springframework.cloud.sleuth.SpanAccessor#getCurrentSpan()
	 */
	public Span getCurrentSpan() {
		return tracer.getCurrentSpan();
	}


	/**
	 * @return
	 * @see org.springframework.cloud.sleuth.SpanAccessor#isTracing()
	 */
	public boolean isTracing() {
		Tracer t = this.tracer;
		while(t instanceof ReactorTrace) {
			t = ((ReactorTrace)t).tracer;
		}
		return t.isTracing();
	}


	/**
	 * @param name
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#createSpan(java.lang.String)
	 */
	public Span createSpan(String name) {
		return tracer.createSpan(name);
	}


	/**
	 * @param name
	 * @param parent
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#createSpan(java.lang.String, org.springframework.cloud.sleuth.Span)
	 */
	public Span createSpan(String name, Span parent) {
		return tracer.createSpan(name, parent);
	}


	/**
	 * @param name
	 * @param sampler
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#createSpan(java.lang.String, org.springframework.cloud.sleuth.Sampler)
	 */
	public Span createSpan(String name, Sampler sampler) {
		return tracer.createSpan(name, sampler);
	}


	/**
	 * @param span
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#continueSpan(org.springframework.cloud.sleuth.Span)
	 */
	public Span continueSpan(Span span) {
		return tracer.continueSpan(span);
	}


	/**
	 * @param key
	 * @param value
	 * @see org.springframework.cloud.sleuth.Tracer#addTag(java.lang.String, java.lang.String)
	 */
	public void addTag(String key, String value) {
		tracer.addTag(key, value);
	}


	/**
	 * @param span
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#detach(org.springframework.cloud.sleuth.Span)
	 */
	public Span detach(Span span) {
		return tracer.detach(span);
	}


	/**
	 * @param span
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#close(org.springframework.cloud.sleuth.Span)
	 */
	public Span close(Span span) {
		return tracer.close(span);
	}


	/**
	 * @param callable
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#wrap(java.util.concurrent.Callable)
	 */
	public <V> Callable<V> wrap(Callable<V> callable) {
		return tracer.wrap(callable);
	}


	/**
	 * @param runnable
	 * @return
	 * @see org.springframework.cloud.sleuth.Tracer#wrap(java.lang.Runnable)
	 */
	public Runnable wrap(Runnable runnable) {
		return tracer.wrap(runnable);
	}
	
	
	
	
	
}
