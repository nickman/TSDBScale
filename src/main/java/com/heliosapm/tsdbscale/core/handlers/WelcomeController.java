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

import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;

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
import org.springframework.web.bind.annotation.RestController;

import com.heliosapm.tsdbscale.core.TSDBMetricRepository;
import com.heliosapm.tsdbscale.core.metrics.TSDBMetric;
import com.heliosapm.tsdbscale.util.JSONOps;

import reactor.core.publisher.Flux;

@RestController
public class WelcomeController {
	
	@Autowired
	protected TSDBMetricRepository repo = null;

	@GetMapping("/")
	public String welcome() {
		return "Hello World";
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
	

}