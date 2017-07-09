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
package org.springframework.cloud.sleuth.instrument.reactive;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;


/**
 * <p>Title: TracePublisher</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.springframework.cloud.sleuth.instrument.reactive.TracePublisher</code></p>
 */

/**
 * Trace representation of the {@link Publisher}. When the {@code subscribe}
 * method is called the we're wrapping the {@link Subscriber} in a trace representation.
 * The trace representation is thread safe thus we can fully control the
 * span creation / closing process.
 *
 * @author Marcin Grzejszczak
 * @author Stephane Maldini
 * @since 1.0.12
 */
public class TracePublisher<T> implements Publisher<T> {

	private final Publisher<T> delegate;
	private final Tracer tracer;
	private final TraceKeys traceKeys;

	/**
	 * Helper static function to create a {@link TracePublisher}
	 */
	public static <T> TracePublisher<T> from(Publisher<T> publisher, Tracer tracer, TraceKeys traceKeys) {
		return new TracePublisher<>(publisher, tracer, traceKeys);
	}

	protected TracePublisher(Publisher<T> delegate, Tracer tracer, TraceKeys traceKeys) {
		this.delegate = Objects.requireNonNull(delegate, "delegate");
		this.tracer = Objects.requireNonNull(tracer, "tracer");
		this.traceKeys = Objects.requireNonNull(traceKeys, "traceKeys");
	}

	@Override public void subscribe(Subscriber<? super T> s) {
		this.delegate.subscribe(new TraceSubscriber<>(s, this.tracer, this.traceKeys));
	}

	private class TraceSubscriber<V> implements Subscriber<V> {

		private static final String REACTIVE_COMPONENT = "reactive";

		private final Span parent;
		private final Tracer tracer;
		private final TraceKeys traceKeys;
		private final Subscriber<V> actual;

		private Span current;

		public TraceSubscriber(Subscriber<V> s, Tracer tracer, TraceKeys traceKeys) {
			this.actual = Objects.requireNonNull(s, "subscriber");
			this.tracer = Objects.requireNonNull(tracer, "tracer");
			this.traceKeys = Objects.requireNonNull(traceKeys, "traceKeys");
			this.parent = this.tracer.getCurrentSpan();
		}

		@Override
		public void onSubscribe(Subscription s) {
			Span span = createSpan();
			if (!span.tags().containsKey(Span.SPAN_LOCAL_COMPONENT_TAG_NAME)) {
				this.tracer.addTag(Span.SPAN_LOCAL_COMPONENT_TAG_NAME, REACTIVE_COMPONENT);
			}
			this.tracer.addTag(this.traceKeys.getAsync().getPrefix()
					+ this.traceKeys.getAsync().getThreadNameKey(), Thread.currentThread().getName());
			this.current = span;
			this.actual.onSubscribe(s);
		}

		private Span createSpan() {
			if (this.parent == null) {
				return this.tracer.createSpan(REACTIVE_COMPONENT);
			}
			return this.tracer.continueSpan(this.parent);
		}

		@Override
		public void onNext(V v) {
			this.actual.onNext(v);
		}

		@Override
		public void onError(Throwable t) {
			try {
				this.actual.onError(t);
			} finally {
				this.tracer.close(this.current);
			}
		}

		@Override
		public void onComplete() {
			try {
				this.actual.onComplete();
			} finally {
				this.tracer.close(this.current);
			}
		}
	}
}
