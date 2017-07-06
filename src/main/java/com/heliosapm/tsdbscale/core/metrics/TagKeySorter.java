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
package com.heliosapm.tsdbscale.core.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * <p>Title: TagKeySorter</p>
 * <p>Description: Sorts tags, putting <b><code>dc</code></b>, <b><code>host</code></b> and <b><code>app</code></b> first</p> </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.metrics.TagKeySorter</code></p>
 */

public class TagKeySorter implements Comparator<String> {
	/** A shareable instance */
	public static final TagKeySorter INSTANCE = new TagKeySorter(); 
	
	
	/**
	 * <p>Title: TagMap</p>
	 * <p>Description: An extension of {@link TreeMap} that automatically adds the tag key sorter</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.tracing.TagKeySorter.TagMap</code></p>
	 */
	public static class TagMap extends TreeMap<String, String> {

		/**  */
		private static final long serialVersionUID = -7663986082893970141L;

		/**
		 * Creates a new TagMap
		 */
		public TagMap() {
			super(INSTANCE);
		}

		/**
		 * Creates a new TagMap
		 * @param m A map to copy into this map
		 */
		public TagMap(final Map<? extends String, ? extends String> m) {
			this();
			if(m!=null) putAll(m);
		}
		
	}
	
	/**
	 * Creates a new TagKeySorter
	 */
	private TagKeySorter() {
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(final String o1, final String o2) {
		return Sorted.eval(o1, o2);
	}
	
	protected static enum Sorted {
		dcdc(0),
		dchost(-1),
		dcapp(-1),
		hostdc(1),
		hosthost(0),
		hostapp(-1),
		appdc(1),
		apphost(1),
		appapp(0);
		
		private static final Set<String> sortKeys = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("dc", "host", "app")));
		
		private Sorted(final int comp) {
			this.comp = comp;
		}
		
		public static int eval(final String o1, final String o2) {
			final String a = o1.toLowerCase();
			final String b = o2.toLowerCase();
			try {
				return Sorted.valueOf(a + b).comp;
			} catch (Exception ex) {
				if(sortKeys.contains(a)) return -1;
				if(sortKeys.contains(b)) return 1;
				return a.compareTo(b);
			}
		}
		
		public final int comp;
	}
	
	/**
	 * Performs the tag key specific compare
	 * @param var1 The first tag
	 * @param var2 The second tag
     * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second.
	 */
	protected static int doCompare(final String var1, final String var2) {
        return "app".equalsIgnoreCase(var1) ? -1 :("app".equalsIgnoreCase(var2)
        	? 1 : ("host".equalsIgnoreCase(var1)?-1:("host".equalsIgnoreCase(var2)?1:2147483647)));
    }


}
