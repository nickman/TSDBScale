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
package com.heliosapm.tsdbscale.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.regex.Pattern;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.ExceptionMessageErrorParser;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanReporter;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.instrument.web.HttpTraceKeysInjector;

import org.springframework.cloud.sleuth.instrument.web.TraceFilter;
import org.springframework.cloud.sleuth.instrument.web.TraceRequestAttributes;
import org.springframework.cloud.sleuth.sampler.AlwaysSampler;
import org.springframework.cloud.sleuth.sampler.NeverSampler;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.server.HandlerFilterFunction;
import org.springframework.web.reactive.function.server.HandlerFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;


import com.heliosapm.tsdbscale.reactor.ReactorTrace;

import reactor.core.publisher.Mono;

/**
 * <p>Title: ReactiveServerRequestTraceFilter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.ReactiveServerRequestTraceFilter</code></p>
 */ 
@Component
public class ReactiveServerRequestTraceFilter implements HandlerFilterFunction<ServerResponse, ServerResponse> {
	
	
	protected ReactorTrace rtracer = null;
	protected final Pattern skipPattern;
	protected HttpTraceKeysInjector keysInjector;
	protected final ReactiveServerHttpSpanExtractor extractor = new ReactiveServerHttpSpanExtractor();
	protected final ExceptionMessageErrorParser errorParser = new ExceptionMessageErrorParser();
	
	
	private static final Logger log = LoggerFactory.getLogger(ReactiveServerRequestTraceFilter.class);
	
	private static final String HTTP_COMPONENT = "http";

	/**
	 * If you register your filter before the {@link TraceFilter} then you will not
	 * have the tracing context passed for you out of the box. That means that e.g. your
	 * logs will not get correlated.
	 */
	public static final int ORDER = Ordered.HIGHEST_PRECEDENCE + 5;

	protected static final String TRACE_REQUEST_ATTR = TraceFilter.class.getName()
			+ ".TRACE";

	protected static final String TRACE_ERROR_HANDLED_REQUEST_ATTR = TraceFilter.class.getName()
			+ ".ERROR_HANDLED";

	protected static final String TRACE_CLOSE_SPAN_REQUEST_ATTR = TraceFilter.class.getName()
			+ ".CLOSE_SPAN";
	
	
	public ReactiveServerRequestTraceFilter(Pattern skipPattern) {
		this.skipPattern = skipPattern;		
	}
	
	public ReactiveServerRequestTraceFilter() {
		this(null);
	}
	
	@Autowired
	public void setTracer(ReactorTrace rtracer) {
		this.rtracer = rtracer;
		keysInjector = new HttpTraceKeysInjector(rtracer.tracer(), rtracer.traceKeys());
	}

//	@Override
	public Mono<ServerResponse> filterX(ServerRequest request, HandlerFunction<ServerResponse> next) {	
		Span incomingSpan = extractor.joinTrace(new ServerRequestSpanTextMap(request));
		final Span span = rtracer.tracer().createSpan("in-reactive-http:/" + request.uri().toString(), incomingSpan);
		Mono<ServerResponse> upresponse = next.handle(request);	
		Mono<ServerResponse> response = rtracer.trace(upresponse, "out-reactive-http:/" + request.uri().toString() , "ReactiveHTTPServer");
		return response.doFinally(sig -> {
			rtracer.tracer().continueSpan(span);
			span.tag("sig", sig.name());
			span.tag("thread", Thread.currentThread().getName());
			rtracer.tracer().close(span);
		});
	}
	
	@Override
	public Mono<ServerResponse> filter(ServerRequest request, HandlerFunction<ServerResponse> next) {
		String uri = request.uri().toASCIIString();
		boolean skip = (skipPattern==null ? false : skipPattern.matcher(uri).matches())
				|| Span.SPAN_NOT_SAMPLED.equals(request.headers().asHttpHeaders().get(Span.SAMPLED_NAME));
		if(skip) return next.handle(request);
		final Span[] spanFromRequest = new Span[1];
//		final ServerResponse[] response = new ServerResponse[1];
//		if (spanFromRequest[0] != null) {
//			continueSpan(request, spanFromRequest[0]);
//		}
//		if (log.isDebugEnabled()) {
//			log.debug("Received a request to uri [" + uri + "] that should not be sampled [" + skip + "]");
//		}
//		// in case of a response with exception status a exception controller will close the span
//		if (isSpanContinued(request)) {
//			return processErrorRequest(request, next, spanFromRequest[0]);
//		}
		String name = HTTP_COMPONENT + ":" + uri;
		//spanFromRequest[0] = createSpan(request, skip, getSpanFromAttribute(request), name);
		spanFromRequest[0] = createSpan(request, skip, this.extractor.joinTrace(new ServerRequestSpanTextMap(request)), name);
		
		Throwable exception = null;
		Mono<ServerResponse> deferredResponse = next.handle(request);
		
		return rtracer.trace(deferredResponse, name, "http:/in");
//				.doOnError(err -> {
//					errorParser.parseErrorTags(rtracer.tracer().getCurrentSpan(), err);
//				})
//				.doOnSuccess(resp -> {					
//					spanFromRequest[0] = createSpanIfRequestNotHandled(request, spanFromRequest[0], name, skip);					
//					detachOrCloseSpans(request, resp, spanFromRequest[0], exception);					
//				})
//				.doOnError(err -> {
//					log.error("Request Failure", err);
//				})
//				.doFinally(sig -> {
//				});
				
	}
		
		private void detachOrCloseSpans(ServerRequest request,
				ServerResponse response, Span spanFromRequest, Throwable exception) {
			Span span = spanFromRequest;
			if (span != null) {
				addResponseTags(response, exception);
				if (span.hasSavedSpan() && requestHasAlreadyBeenHandled(request)) {
					recordParentSpan(span.getSavedSpan());
				} else if (!requestHasAlreadyBeenHandled(request)) {
					span = rtracer.tracer().close(span);
				}
				recordParentSpan(span);
				// in case of a response with exception status will close the span when exception dispatch is handled
				// checking if tracing is in progress due to async / different order of view controller processing
				if (httpStatusSuccessful(response) && rtracer.tracer().isTracing()) {
					if (log.isDebugEnabled()) {
						log.debug("Closing the span " + span + " since the response was successful");
					}
					rtracer.tracer().close(span);
				} else if (errorAlreadyHandled(request) && rtracer.tracer().isTracing()) {
					if (log.isDebugEnabled()) {
						log.debug(
								"Won't detach the span " + span + " since error has already been handled");
					}
				}  else if (shouldCloseSpan(request) && rtracer.tracer().isTracing() && stillTracingCurrentSapn(span)) {
					if (log.isDebugEnabled()) {
						log.debug(
								"Will close span " + span + " since some component marked it for closure");
					}
					rtracer.tracer().close(span);
				} else if (rtracer.tracer().isTracing()) {
					if (log.isDebugEnabled()) {
						log.debug("Detaching the span " + span + " since the response was unsuccessful");
					}
					rtracer.tracer().detach(span);
				}
			}
		}
		
		private boolean stillTracingCurrentSapn(Span span) {
			return rtracer.tracer().getCurrentSpan().equals(span);
		}
		
		
		private boolean errorAlreadyHandled(ServerRequest request) {
			return Boolean.valueOf(
					String.valueOf(request.attribute(TRACE_ERROR_HANDLED_REQUEST_ATTR)));
		}
		
		private boolean shouldCloseSpan(ServerRequest request) {
			return Boolean.valueOf(
					String.valueOf(request.attribute(TRACE_CLOSE_SPAN_REQUEST_ATTR)));
		}

		
		
		private void recordParentSpan(Span parent) {
			if (parent == null) {
				return;
			}
			if (parent.isRemote()) {
				if (log.isDebugEnabled()) {
					log.debug("Trying to send the parent span " + parent + " to Zipkin");
				}
				parent.stop();
				// should be already done by HttpServletResponse wrappers
				annotateWithServerSendIfLogIsNotAlreadyPresent(parent);
				rtracer.spanReporter().report(parent);
			} else {
				// should be already done by HttpServletResponse wrappers
				annotateWithServerSendIfLogIsNotAlreadyPresent(parent);
			}
		}		
		
		private boolean httpStatusSuccessful(ServerResponse response) {
			HttpStatus status = response.statusCode();
			HttpStatus.Series httpStatusSeries = status.series();
			return httpStatusSeries == HttpStatus.Series.SUCCESSFUL || httpStatusSeries == HttpStatus.Series.REDIRECTION;
		}		
		
		static void annotateWithServerSendIfLogIsNotAlreadyPresent(Span span) {
			if (span == null) {
				return;
			}
			for (org.springframework.cloud.sleuth.Log log1 : span.logs()) {
				if (Span.SERVER_SEND.equals(log1.getEvent())) {
					if (log.isTraceEnabled()) {
						log.trace("Span was already annotated with SS, will not do it again");
					}
					return;
				}
			}
			if (log.isTraceEnabled()) {
				log.trace("Will set SS on the span");
			}
			span.logEvent(Span.SERVER_SEND);
		}
		
		
		private Span createSpanIfRequestNotHandled(ServerRequest request,
				Span spanFromRequest, String name, boolean skip) {
			if (!requestHasAlreadyBeenHandled(request)) {
				spanFromRequest = rtracer.tracer().createSpan(name);
				request.attributes().put(TRACE_REQUEST_ATTR, spanFromRequest);
				if (log.isDebugEnabled() && !skip) {
					log.debug("The	 request with uri [" + request.uri() + "] hasn't been handled by any of Sleuth's components. "
							+ "That means that most likely you're using custom HandlerMappings and didn't add Sleuth's TraceHandlerInterceptor. "
							+ "Sleuth will create a span to ensure that the graph of calls remains valid in Zipkin");
				}
			}
			return spanFromRequest;
		}
		
		private boolean requestHasAlreadyBeenHandled(ServerRequest request) {
			return request.attribute(TraceRequestAttributes.HANDLED_SPAN_REQUEST_ATTR) != null;
		}
		
		
		/**
		 * Creates a span and appends it as the current request's attribute
		 */
		private Span createSpan(ServerRequest request,
				boolean skip, Span spanFromRequest, String name) {
			if (spanFromRequest != null) {
				if (log.isDebugEnabled()) {
					log.debug("Span has already been created - continuing with the previous one");
				}
				return spanFromRequest;
			}
			Span parent = extractor.joinTrace(new ServerRequestSpanTextMap(request));
			if (parent != null) {
				if (log.isDebugEnabled()) {
					log.debug("Found a parent span " + parent + " in the request");
				}
				addRequestTagsForParentSpan(request, parent);
				spanFromRequest = parent;
				rtracer.tracer().continueSpan(spanFromRequest);
				if (parent.isRemote()) {
					parent.logEvent(Span.SERVER_RECV);
				}
				request.attributes().put(TRACE_REQUEST_ATTR, spanFromRequest);
				if (log.isDebugEnabled()) {
					log.debug("Parent span is " + parent + "");
				}
			} else {
				if (skip) {
					spanFromRequest = rtracer.tracer().createSpan(name, NeverSampler.INSTANCE);
				}
				else {
					String header = request.headers().asHttpHeaders().toSingleValueMap().get(Span.SPAN_FLAGS);
					if (Span.SPAN_SAMPLED.equals(header)) {
						spanFromRequest = rtracer.tracer().createSpan(name, new AlwaysSampler());
					} else {
						spanFromRequest = rtracer.tracer().createSpan(name);
					}
				}
				spanFromRequest.logEvent(Span.SERVER_RECV);
				request.attributes().put(TRACE_REQUEST_ATTR, spanFromRequest);
				if (log.isDebugEnabled()) {
					log.debug("No parent span present - creating a new span");
				}
			}
			return spanFromRequest;
		}
		
		/** Override to add annotations not defined in {@link TraceKeys}. */
		protected void addRequestTags(Span span, ServerRequest request) {
			String uri = request.uri().toASCIIString();
			rtracer.keysInjector().addRequestTags(span, getFullUrl(request),
					request.uri().getHost(), uri, request.method().name());
			for (String name : rtracer.traceKeys().getHttp().getHeaders()) {
				Enumeration<String> values = Collections.enumeration(request.headers().asHttpHeaders().toSingleValueMap().values());
				if (values.hasMoreElements()) {
					String key = rtracer.traceKeys().getHttp().getPrefix() + name.toLowerCase();
					ArrayList<String> list = Collections.list(values);
					String value = list.size() == 1 ? list.get(0)
							: StringUtils.collectionToDelimitedString(list, ",", "'", "'");
					rtracer.keysInjector().tagSpan(span, key, value);
				}
			}
		}
		
		private String getFullUrl(ServerRequest request) {
			StringBuffer requestURI = new StringBuffer(request.uri().toString());
			String queryString = request.uri().getRawQuery();
			if (queryString == null) {
				return requestURI.toString();
			} else {
				return requestURI.append('?').append(queryString).toString();
			}
		}
		
		
		/**
		 * In order not to send unnecessary data we're not adding request tags to the server
		 * side spans. All the tags are there on the client side.
		 */
		private void addRequestTagsForParentSpan(ServerRequest request, Span spanFromRequest) {
			if (spanFromRequest.getName().contains("parent")) {
				addRequestTags(spanFromRequest, request);
			}
		}		
		
		
		private Mono<ServerResponse> processErrorRequest(ServerRequest request, HandlerFunction<ServerResponse> next, Span spanFromRequest) {
			if (log.isDebugEnabled()) {
				log.debug("The span " + spanFromRequest + " was already detached once and we're processing an error");
			}
			Mono<ServerResponse> deferredResponse = null;
			try {
				deferredResponse = next.handle(request);				
			} finally {
				request.attributes().put(TRACE_ERROR_HANDLED_REQUEST_ATTR, true);
				addDeferredResponseTags(deferredResponse, null);
				if (request.attribute(TraceRequestAttributes.ERROR_HANDLED_SPAN_REQUEST_ATTR) == null) {
					rtracer.tracer().close(spanFromRequest);
				}
			}
			return deferredResponse;
		}
		
		
		
		/** Override to add annotations not defined in {@link TraceKeys}. */
		protected void addDeferredResponseTags(final Mono<ServerResponse> deferredResponse, final Throwable e) {
			deferredResponse.subscribe(sr -> {
				addResponseTags(sr, e);
			});
		}
		
		/** Override to add annotations not defined in {@link TraceKeys}. */
		protected void addResponseTags(ServerResponse response, Throwable e) {
			int httpStatus = response.statusCode().value();
			if (httpStatus == HttpStatus.OK.value() && e != null) {
				// Filter chain threw exception but the response status may not have been set
				// yet, so we have to guess.
				rtracer.tracer().addTag(rtracer.traceKeys().getHttp().getStatusCode(),
						String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR));
			}
			// only tag valid http statuses
			else if (httpStatus >= 100 && (httpStatus < 200) || (httpStatus > 399)) {
				rtracer.tracer().addTag(rtracer.traceKeys().getHttp().getStatusCode(),
						String.valueOf(response.statusCode().name()));
			}				
		}
		

	
		private Span getSpanFromAttribute(ServerRequest request) {
			return (Span) request.attribute(TRACE_REQUEST_ATTR).orElse(null);		
		}

		private void continueSpan(ServerRequest request, Span spanFromRequest) {
			rtracer.tracer().continueSpan(spanFromRequest);
			request.attributes().put(TraceRequestAttributes.SPAN_CONTINUED_REQUEST_ATTR, "true");
			if (log.isDebugEnabled()) {
				log.debug("There has already been a span in the request " + spanFromRequest);
			}
		}		
	
		private boolean isSpanContinued(ServerRequest request) {
			return getSpanFromAttribute(request) != null;
		}
		
		private Span parentSpan(Span span) {
			if (span == null) {
				return null;
			}
			if (span.hasSavedSpan()) {
				return span.getSavedSpan();
			}
			return span;
		}
		
		

}
