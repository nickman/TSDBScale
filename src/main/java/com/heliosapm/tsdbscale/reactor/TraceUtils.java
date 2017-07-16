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
package com.heliosapm.tsdbscale.reactor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.cloud.sleuth.Span;

import com.heliosapm.tsdbscale.util.JSONOps;

/**
 * <p>Title: TraceUtils</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.reactor.TraceUtils</code></p>
 */

public class TraceUtils {
	public static final byte JSON_OBJ_OPENER = '{';
	
	public static Map<String, String> fromString(String s) {
		final Map<String, String> map = new HashMap<String, String>();
		if(s==null || s.trim().isEmpty()) return map;
		s = s.trim();
		if(s.indexOf(JSON_OBJ_OPENER)==0) {
			if(s.replace(" ", "").charAt(1)=='"') {
				return JSONOps.parseToObject(s, JSONOps.TR_STR_STR_HASH_MAP);
			} else {
				s = s.substring(1, s.length()-1);
				for(String ps: splitString(s, ',', true)) {
					String[] kv = splitString(ps, '=', true);
					map.put(kv[0], kv[1]);
				}
			}
		} else {			
			for(String ps: splitString(s, ',', true)) {
				String[] kv = splitString(ps, '=', true);
				map.put(kv[0], kv[1]);
			}
		}
		return map;
	}
	
	
	/**
	 * Optimized version of {@code String#split} that doesn't use regexps.
	 * This function works in O(5n) where n is the length of the string to
	 * split.
	 * @param s The string to split.
	 * @param c The separator to use to split the string.
	 * @param trimBlanks true to not return any whitespace only array items
	 * @return A non-null, non-empty array.
	 * <p>Copied from <a href="http://opentsdb.net">OpenTSDB</a>.
	 */
	public static String[] splitString(final String s, final char c, final boolean trimBlanks) {
		final char[] chars = s.toCharArray();
		int num_substrings = 1;
		final int last = chars.length-1;
		for(int i = 0; i <= last; i++) {
			char x = chars[i];
			if (x == c) {
				num_substrings++;
			}
		}
		final String[] result = new String[num_substrings];
		final int len = chars.length;
		int start = 0;  // starting index in chars of the current substring.
		int pos = 0;    // current index in chars.
		int i = 0;      // number of the current substring.
		for (; pos < len; pos++) {
			if (chars[pos] == c) {
				result[i++] = new String(chars, start, pos - start);
				start = pos + 1;
			}
		}
		result[i] = new String(chars, start, pos - start);
		if(trimBlanks) {
			int blanks = 0;
			final List<String> strs = new ArrayList<String>(result.length);
			for(int x = 0; x < result.length; x++) {
				if(result[x].trim().isEmpty()) {
					blanks++;
				} else {
					strs.add(result[x]);
				}
			}
			if(blanks==0) return result;
			return strs.toArray(new String[result.length - blanks]);
		}
		return result;
	}
	
	public static Map<String, String> removeKeyPrefix(Map<String, String> map, String prefix) {
		Map<String, String> tmp = new HashMap<String, String>(map);
		map.clear();
		for (Map.Entry<String, String> entry : tmp.entrySet()) {
			String key = entry.getKey();
			if(key.indexOf(prefix)==0) {
				key = unprefixedKey(key, prefix);				
			}
			map.put(key, entry.getValue());
		}
		return map;
	}
	
	public static String unprefixedKey(String key, String prefix) {
		return key.substring(key.indexOf(prefix) + 1);
	}
	
	public static void main(String[] args) {
		HashMap<String, String> map = new HashMap<>();
		map.put("foo", "bar");
		map.put("sn,a", "fu");
		log("Map: [" + map + "]");
		
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}
	
}
