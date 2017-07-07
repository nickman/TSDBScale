/**
 * 
 */
package com.github.pgasync.impl.conversion;

import java.util.function.BiFunction;
import java.util.function.Function;

import com.github.pgasync.impl.Oid;

/**
 * @author nwhitehead
 *
 */
public class ArrayConverter {
	public static byte[] fromArray(final Object elements, final Function<Object,byte[]> printFn) {
		return com.github.pgasync.impl.conversion.ArrayConversions.fromArray(elements, printFn);
	}
	
	public static <T> T toArray(Class<T> type, Oid oid, byte[] value, BiFunction<Oid,byte[],Object> parse) {
		return com.github.pgasync.impl.conversion.ArrayConversions.toArray(type, oid, value, parse);
	}
}
