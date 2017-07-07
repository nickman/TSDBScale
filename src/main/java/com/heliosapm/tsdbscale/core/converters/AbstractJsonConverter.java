/**
 * 
 */
package com.heliosapm.tsdbscale.core.converters;

import java.util.Collections;
import java.util.EnumSet;

import com.github.pgasync.Converter;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.Oid;
import com.heliosapm.tsdbscale.util.JSONOps;

/**
 * @author nwhitehead
 * @param <T> The conversion type
 *
 */
public abstract class AbstractJsonConverter<T> implements Converter<T> {
	protected final Class<T> type;
	protected final EnumSet<Oid> oids;
	
	
	
	/**
	 * Creates a new converter
	 * @param type The target conversion type
	 * @param oids The mapped oids
	 */
	public AbstractJsonConverter(Class<T> type, Oid...oids) {
		this.type = type;
		this.oids = EnumSet.noneOf(Oid.class);
		Collections.addAll(this.oids, oids);
	}

	/**
	 * {@inheritDoc}
	 * @see com.github.pgasync.Converter#type()
	 */
	@Override
	public Class<T> type() {
		return type;
	}

	/**
	 * {@inheritDoc}
	 * @see com.github.pgasync.Converter#from(java.lang.Object)
	 */
	@Override
	public byte[] from(T o) {
		return JSONOps.serializeToBytes(o);
	}

	/**
	 * {@inheritDoc}
	 * @see com.github.pgasync.Converter#to(com.github.pgasync.impl.Oid, byte[])
	 */
	@Override
	public T to(Oid oid, byte[] value) {
		if(oids.contains(oid)) {
			return JSONOps.parseToObject(value, type);
		}
   		throw new SqlException("Unsupported conversion " + oid.name() + " -> JsonNode");        
	}

}
