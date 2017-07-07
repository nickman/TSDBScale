/**
 * 
 */
package org.springframework.cloud.sleuth.reactor;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;
import reactor.util.context.Contextualized;

/**
 * @author nwhitehead
 *
 */
public class SpanSubscriber extends AtomicBoolean
implements Subscriber<Object>, Subscription, Contextualized {

	private static final Logger log = Loggers.getLogger(SpanSubscriber.class);

	private final Span span;
	private final Span rootSpan;
	private final Subscriber<? super Object> subscriber;
	private final Context context;
	private final Tracer tracer;
	private Subscription s;

	public SpanSubscriber(Subscriber<? super Object> subscriber, Context ctx, Tracer tracer,
			String name) {
		this.subscriber = subscriber;
		this.tracer = tracer;
		Span root = ctx.getOrDefault(Span.class, tracer.getCurrentSpan());
		if (log.isInfoEnabled()) {
			log.info("Span from context [{}]", root);
		}
		this.rootSpan = root;
		if (log.isInfoEnabled()) {
			log.info("Stored context root span [{}]", this.rootSpan);
		}
		this.span = tracer.createSpan(name, root);
		if (log.isInfoEnabled()) {
			log.info("Created span [{}], with name [{}]", this.span, name);
		}
		this.context = ctx.put(Span.class, this.span);
	}

	@Override public void onSubscribe(Subscription subscription) {
		if (log.isInfoEnabled()) {
			log.info("On subscribe");
		}
		this.s = subscription;
		this.tracer.continueSpan(this.span);
		if (log.isInfoEnabled()) {
			log.info("On subscribe - span continued");
		}
		this.subscriber.onSubscribe(this);
	}

	@Override public void request(long n) {
		if (log.isInfoEnabled()) {
			log.info("Request");
		}
		this.tracer.continueSpan(this.span);
		if (log.isInfoEnabled()) {
			log.info("Request - continued");
		}
		this.s.request(n);
		// We're in the main thread so we don't want to pollute it with wrong spans
		// that's why we need to detach the current one and continue with its parent
		Span localRootSpan = this.span;
		while (localRootSpan != null) {
			if (this.rootSpan != null) {
				if (localRootSpan.getSpanId() != this.rootSpan.getSpanId() &&
						!isRootParentSpan(localRootSpan)) {
					localRootSpan = continueDetachedSpan(localRootSpan);
				} else {
					localRootSpan = null;
				}
			} else if (!isRootParentSpan(localRootSpan)) {
				localRootSpan = continueDetachedSpan(localRootSpan);
			} else {
				localRootSpan = null;
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Request after cleaning. Current span [{}]",
					this.tracer.getCurrentSpan());
		}
	}

	private boolean isRootParentSpan(Span localRootSpan) {
		return localRootSpan.getSpanId() == localRootSpan.getTraceId();
	}

	private Span continueDetachedSpan(Span localRootSpan) {
		if (log.isInfoEnabled()) {
			log.info("Will detach span {}", localRootSpan);
		}
		Span detachedSpan = this.tracer.detach(localRootSpan);
		return this.tracer.continueSpan(detachedSpan);
	}

	@Override public void cancel() {
		try {
			if (log.isInfoEnabled()) {
				log.info("Cancel");
			}
			this.s.cancel();
		}
		finally {
			cleanup();
		}
	}

	@Override public void onNext(Object o) {
		this.subscriber.onNext(o);
	}

	@Override public void onError(Throwable throwable) {
		try {
			this.subscriber.onError(throwable);
		}
		finally {
			cleanup();
		}
	}

	@Override public void onComplete() {
		try {
			this.subscriber.onComplete();
		}
		finally {
			cleanup();
		}
	}

	void cleanup() {
		if (compareAndSet(false, true)) {
			if (log.isInfoEnabled()) {
				log.info("Cleaning up");
			}
			if (this.tracer.getCurrentSpan() != this.span) {
				if (log.isInfoEnabled()) {
					log.info("Detaching span");
				}
				this.tracer.detach(this.tracer.getCurrentSpan());
				this.tracer.continueSpan(this.span);
				if (log.isInfoEnabled()) {
					log.info("Continuing span");
				}
			}
			if (log.isInfoEnabled()) {
				log.info("Closing span");
			}
			this.tracer.close(this.span);
			if (log.isInfoEnabled()) {
				log.info("Span closed");
			}
			if (this.rootSpan != null) {
				this.tracer.continueSpan(this.rootSpan);
				this.tracer.close(this.rootSpan);
				if (log.isInfoEnabled()) {
					log.info("Closed root span");
				}
			}
		}
	}

	@Override 
	public Context currentContext() {
		return this.context;
	}
}