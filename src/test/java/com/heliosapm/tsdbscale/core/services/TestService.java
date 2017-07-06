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
package com.heliosapm.tsdbscale.core.services;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

/**
 * <p>Title: TestService</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.services.TestService</code></p>
 */
@Service
public class TestService {
	private static final AtomicLong opSerial = new AtomicLong(0L);
	
	private static final Logger log = LoggerFactory.getLogger(TestService.class);
	/**
	 * Creates a new TestService
	 */
	
	public TestService() {
		// TODO Auto-generated constructor stub
	}
	
	@Retryable
	public String reverse(final String value) {
		final long id = opSerial.incrementAndGet();
		if(id%2!=0) {
			log.info("Failing reverse on op id: {}", id);
			throw new IllegalArgumentException();
		}
		log.info("Passing reverse on op id: {}", id);
		return new StringBuilder(value).reverse().toString();
	}
	

}
