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
package com.heliosapm.tsdbscale.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

/**
 * <p>Title: JSONOps</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.util.JSONOps</code></p>
 */

public class JSONOps {
	  public static class JSONException extends RuntimeException {

		    /**  */
		    private static final long serialVersionUID = 4607480986117102076L;

		    /**
		     * Creates a new JsonException
		     */
		    public JSONException() {
		      super();
		    }

		    /**
		     * Creates a new JsonException
		     * 
		     * @param message
		     *          The json exception message
		     */
		    public JSONException(String message) {
		      super(message);
		    }

		    /**
		     * Creates a new JsonException
		     * 
		     * @param message
		     *          The json exception message
		     * @param cause
		     *          The underlying cause
		     */
		    public JSONException(String message, Throwable cause) {
		      super(message, cause);
		    }

		    /**
		     * Creates a new JsonException
		     * 
		     * @param cause
		     *          The underlying cause
		     */
		    public JSONException(final Throwable cause) {
		      super(cause);
		    }

		  }

		  /** Sharable object mapper */
		  private static final ObjectMapper jsonMapper = new ObjectMapper();
		  /** Sharable json factory */
		  private static final JsonFactory jsonFactory = jsonMapper.getFactory();
		  /** Sharable json serializer provider */
		  private static final SerializerProvider serProvider = jsonMapper.getSerializerProviderInstance();
		  
		  /** Type reference for common string/object maps */
		  public static final TypeReference<HashMap<String, Object>> TR_STR_OBJ_HASH_MAP = 
		    new TypeReference<HashMap<String, Object>>() {};
		  /** Type reference for common string/string maps */
		  public static final TypeReference<HashMap<String, String>> TR_STR_STR_HASH_MAP = 
		    new TypeReference<HashMap<String, String>>() {};
		  

		  /**
		   * Returns the shared JSON ObjectMapper
		   * 
		   * @return the shared JSON ObjectMapper
		   */
		  public static ObjectMapper objectMapper() {
		    return jsonMapper;
		  }
		  
		  

		  /**
		   * Parses the passed channel buffer into a JsonNode
		   * 
		   * @param buff
		   *          The buffer to parse
		   * @return the parsed JsonNode
		   */
		  public static JsonNode parseToNode(final ByteBuf buff) {
		    return parseToNode(buff, false);
		  }
		  

		  /**
		   * Parses the passed channel buffer into a JsonNode
		   * 
		   * @param buff
		   *          The buffer to parse
		   * @param nullIfNoContent
		   *          If true, returns null if no content is available to parse
		   * @return the parsed JsonNode
		   */
		  public static JsonNode parseToNode(final ByteBuf buff, final boolean nullIfNoContent) {
		    if (buff == null || buff.readableBytes() < 1) {
		      if (nullIfNoContent)
		        return null;
		      throw new IllegalArgumentException("Incoming data was null");
		    }
		    final InputStream is = new ByteBufInputStream(buff);
		    try {
		      return parseToNode(is);
		    } catch (Exception e) {
		      if (nullIfNoContent)
		        return null;
		      throw new JSONException(e);
		    } finally {
		      try {
		        is.close();
		      } catch (Exception x) {
		        /* No Op */}
		    }
		  }

		  /**
		   * Reads and parses the data from the passed input stream into a JsonNode
		   * 
		   * @param is
		   *          The input stream to read from
		   * @return the parsed JsonNode
		   */
		  public static JsonNode parseToNode(final InputStream is) {
		    if (is == null)
		      throw new IllegalArgumentException("InputStream was null");
		    try {
		      return jsonMapper.readTree(is);
		    } catch (Exception e) {
		      throw new JSONException(e);
		    }
		  }

		  /**
		   * Reads and parses the data from the passed URL into a JsonNode
		   * 
		   * @param url
		   *          The url to read from
		   * @return the parsed JsonNode
		   */
		  public static JsonNode parseToNode(final URL url) {
		    if (url == null)
		      throw new IllegalArgumentException("URL was null");
		    InputStream is = null;
		    try {
		      is = url.openStream();
		      return parseToNode(is);
		    } catch (Exception e) {
		      throw new JSONException(e);
		    } finally {
		      if (is != null)
		        try {
		          is.close();
		        } catch (Exception x) {
		          /* No Op */}
		    }
		  }

		  /**
		   * Deserializes a JSON formatted byte array to a specific class type <b>Note:</b> If you get mapping exceptions you
		   * may need to provide a TypeReference
		   * 
		   * @param json
		   *          The byte array to deserialize from
		   * @param pojo
		   *          The class type of the object used for deserialization
		   * @return An object of the {@code pojo} type
		   * @throws IllegalArgumentException
		   *           if the data or class was null or parsing failed
		   * @throws JSONException
		   *           if the data could not be parsed
		   */
		  public static final <T> T parseToObject(final byte[] json, final Class<T> pojo) {
		    return parseToObject(json, 0, json.length, pojo);
		  }

		  /**
		   * Deserializes a JSON formatted byte array to a specific class type <b>Note:</b> If you get mapping exceptions you
		   * may need to provide a TypeReference
		   * 
		   * @param json
		   *          The byte array to deserialize from
		   * @param offset
		   *          The offset to read from
		   * @param len
		   *          The number of bytes to read
		   * @param pojo
		   *          The class type of the object used for deserialization
		   * @return An object of the {@code pojo} type
		   * @throws IllegalArgumentException
		   *           if the data or class was null or parsing failed
		   * @throws JSONException
		   *           if the data could not be parsed
		   */
		  public static final <T> T parseToObject(final byte[] json, final int offset, final int length, Class<T> pojo) {
		    if (json == null)
		      throw new IllegalArgumentException("Incoming byte array was null");
		    if (pojo == null)
		      throw new IllegalArgumentException("Missing class type");
		    try {
		      return jsonMapper.readValue(json, offset, length, pojo);
		    } catch (JsonParseException e) {
		      throw new IllegalArgumentException(e);
		    } catch (JsonMappingException e) {
		      throw new IllegalArgumentException(e);
		    } catch (IOException e) {
		      throw new JSONException(e);
		    }
		  }

		  /**
		   * Deserializes a JSON formatted byte array to a specific class type <b>Note:</b> If you get mapping exceptions you
		   * may need to provide a TypeReference
		   * 
		   * @param json
		   *          The buffer to deserialize from
		   * @param pojo
		   *          The class type of the object used for deserialization
		   * @return An object of the {@code pojo} type
		   * @throws IllegalArgumentException
		   *           if the data or class was null or parsing failed
		   * @throws JSONException
		   *           if the data could not be parsed
		   */
		  public static final <T> T parseToObject(final ByteBuf json, final Class<T> pojo) {
		    if (json == null)
		      throw new IllegalArgumentException("Incoming buffer was null");
		    if (pojo == null)
		      throw new IllegalArgumentException("Missing class type");
		    InputStream is = new ByteBufInputStream(json);
		    try {
		      return jsonMapper.readValue(is, pojo);
		    } catch (JsonParseException e) {
		      throw new IllegalArgumentException(e);
		    } catch (JsonMappingException e) {
		      throw new IllegalArgumentException(e);
		    } catch (IOException e) {
		      throw new JSONException(e);
		    } finally {
		      if (is != null)
		        try {
		          is.close();
		        } catch (Exception x) {
		          /* No Op */}
		    }
		  }

		  /**
		   * Deserializes a JSON formatted input stream to a specific class type <b>Note:</b> If you get mapping exceptions you
		   * may need to provide a TypeReference
		   * 
		   * @param is
		   *          The input stream to deserialize from
		   * @param pojo
		   *          The class type of the object used for deserialization
		   * @return An object of the {@code pojo} type
		   * @throws IllegalArgumentException
		   *           if the data or class was null or parsing failed
		   * @throws JSONException
		   *           if the data could not be parsed
		   */
		  public static final <T> T parseToObject(final InputStream is, final Class<T> pojo) {
		    if (is == null)
		      throw new IllegalArgumentException("Incoming stream was null");
		    if (pojo == null)
		      throw new IllegalArgumentException("Missing class type");
		    try {
		      return jsonMapper.readValue(is, pojo);
		    } catch (JsonParseException e) {
		      throw new IllegalArgumentException(e);
		    } catch (JsonMappingException e) {
		      throw new IllegalArgumentException(e);
		    } catch (IOException e) {
		      throw new JSONException(e);
		    }
		  }

		  /**
		   * Deserializes a JSON formatted string to a specific class type <b>Note:</b> If you get mapping exceptions you may
		   * need to provide a TypeReference
		   * 
		   * @param json
		   *          The string to deserialize
		   * @param pojo
		   *          The class type of the object used for deserialization
		   * @return An object of the {@code pojo} type
		   * @throws IllegalArgumentException
		   *           if the data or class was null or parsing failed
		   * @throws JSONException
		   *           if the data could not be parsed
		   */
		  public static final <T> T parseToObject(final JsonNode json, final Class<T> pojo) {
		    if (json == null)
		      throw new IllegalArgumentException("Incoming data was null or empty");
		    if (pojo == null)
		      throw new IllegalArgumentException("Missing class type");

		    try {
		      return jsonMapper.convertValue(json, pojo);
		    } catch (Exception e) {
		      throw new JSONException(e);
		    }
		  }

		  /**
		   * Deserializes a JSON formatted string to a specific class type <b>Note:</b> If you get mapping exceptions you may
		   * need to provide a TypeReference
		   * 
		   * @param json
		   *          The string to deserialize
		   * @param pojo
		   *          The class type of the object used for deserialization
		   * @return An object of the {@code pojo} type
		   * @throws IllegalArgumentException
		   *           if the data or class was null or parsing failed
		   * @throws JSONException
		   *           if the data could not be parsed
		   */
		  public static final <T> T parseToObject(final JsonNode json, final TypeReference<T> pojo) {
		    if (json == null)
		      throw new IllegalArgumentException("Incoming data was null or empty");
		    if (pojo == null)
		      throw new IllegalArgumentException("Missing class type");

		    try {
		      return jsonMapper.convertValue(json, pojo);
		    } catch (Exception e) {
		      throw new JSONException(e);
		    }
		  }

		  /**
		   * Deserializes a JSON formatted string to a specific class type <b>Note:</b> If you get mapping exceptions you may
		   * need to provide a TypeReference
		   * 
		   * @param json
		   *          The string to deserialize
		   * @param pojo
		   *          The class type of the object used for deserialization
		   * @return An object of the {@code pojo} type
		   * @throws IllegalArgumentException
		   *           if the data or class was null or parsing failed
		   * @throws JSONException
		   *           if the data could not be parsed
		   */
		  public static final <T> T parseToObject(final String json, final Class<T> pojo) {
		    if (json == null || json.isEmpty())
		      throw new IllegalArgumentException("Incoming data was null or empty");
		    if (pojo == null)
		      throw new IllegalArgumentException("Missing class type");

		    try {
		      return jsonMapper.readValue(json, pojo);
		    } catch (JsonParseException e) {
		      throw new IllegalArgumentException(e);
		    } catch (JsonMappingException e) {
		      throw new IllegalArgumentException(e);
		    } catch (IOException e) {
		      throw new JSONException(e);
		    }
		  }

		  /**
		   * Registers a deser for the passed class
		   * 
		   * @param clazz
		   *          The class for which a deser is being registered
		   * @param deser
		   *          The deserializer
		   */
		  public static <T> void registerDeserializer(final Class<T> clazz, final JsonDeserializer<T> deser) {
		    if (clazz == null)
		      throw new IllegalArgumentException("The passed class was null");
		    if (deser == null)
		      throw new IllegalArgumentException("The passed deserializer for [" + clazz.getName() + "] was null");
		    final SimpleModule module = new SimpleModule();
		    module.addDeserializer(clazz, deser);
		    jsonMapper.registerModule(module);
		  }

		  /**
		   * Registers a module with the shared json mapper
		   * 
		   * @param module
		   *          The module to register
		   */
		  public static void registerModule(final Module module) {
		    if (module == null)
		      throw new IllegalArgumentException("The passed module was null");
		    jsonMapper.registerModule(module);
		  }

		  /**
		   * Registers a ser for the passed class
		   * 
		   * @param clazz
		   *          The class for which a ser is being registered
		   * @param ser
		   *          The serializer
		   */
		  public static <T> void registerSerializer(final Class<T> clazz, final JsonSerializer<T> ser) {
		    if (clazz == null)
		      throw new IllegalArgumentException("The passed class was null");
		    if (ser == null)
		      throw new IllegalArgumentException("The passed serializer for [" + clazz.getName() + "] was null");
		    final SimpleModule module = new SimpleModule();
		    module.addSerializer(clazz, ser);
		    jsonMapper.registerModule(module);
		  }

		  /**
		   * Serializes the passed object to the passed byte buffer
		   * 
		   * @param object
		   *          The object to serialize
		   * @param buff
		   *          The buffer to write to
		   * @param serializer
		   *          The optional json serializer to use
		   * @return the written buffer
		   */
		  public static <T> ByteBuf serialize(final T object, final ByteBuf buff, final JsonSerializer<T> serializer) {
		    if (object == null)
		      throw new IllegalArgumentException("Object was null");
		    if (buff == null)
		      throw new IllegalArgumentException("ByteBuf was null");
//		    if(object instanceof NettyAdapter<?>) {
//		      ((NettyAdapter<?>)object).intoByteBufJson(object, buff);
//		    }
		    final OutputStream os = new ByteBufOutputStream(buff);
		    try {
		      serialize(object, os, serializer);
		      os.flush();
		      os.close();
		    } catch (Exception ex) {
		      throw new RuntimeException("Failed to write object to buffer", ex);
		    } finally {
		      try {
		        os.close();
		      } catch (Exception x) {
		        /* No Op */}
		    }
		    return buff;
		  }
		  
		  /**
		   * Serializes the passed object to the passed byte buffer
		   * 
		   * @param object
		   *          The object to serialize
		   * @param buff
		   *          The buffer to write to
		   * @return the written buffer
		   */
		  public static ByteBuf serialize(final Object object, final ByteBuf buff) {
		    return serialize(object, buff, null);
		  }
		  

		  /**
		   * Serializes the passed object to the passed output stream
		   * 
		   * @param object
		   *          The object to serialize
		   * @param os
		   *          The output stream to write to
		   * @param serializer
		   *          An optional serializer to use
		   */
		  public static <T> void serialize(final T object, final OutputStream os, final JsonSerializer<T> serializer) {
		    if (object == null)
		      throw new IllegalArgumentException("Object was null");
		    if (os == null)
		      throw new IllegalArgumentException("OutputStream was null");
		    try {
		      if(serializer!=null) {
		        JsonGenerator gen = jsonFactory.createGenerator(os);
		        try {
		          serializer.serialize(object, gen, serProvider);
		          gen.flush();
		        } finally {
		          try { gen.close(); } catch (Exception x) {/* No Op */}
		        }        
		      } else {
		        jsonMapper.writeValue(os, object);
		      }
		      os.flush();
		    } catch (Exception ex) {
		      throw new JSONException(ex);
		    }
		  }

		  /**
		   * Serializes the passed object to the passed output stream
		   * 
		   * @param object
		   *          The object to serialize
		   * @param os
		   *          The output stream to write to
		   */
		  public static void serialize(final Object object, final OutputStream os) {
		    serialize(object, os, null);
		  }
		  
		  /**
		   * Converts the passed object to a JsonNode
		   * 
		   * @param object
		   *          The object to convert
		   * @return the resulting JsonNode
		   */
		  public static final JsonNode serializeToNode(final Object object) {
		    if (object == null)
		      throw new IllegalArgumentException("Object was null");
		    return jsonMapper.convertValue(object, JsonNode.class);
		  }

		  /**
		   * Serializes the node to a string
		   * 
		   * @param node
		   *          The node to serialize
		   * @return the written string
		   */
		  public static String serializeToString(final JsonNode node) {
		    if (node == null)
		      throw new IllegalArgumentException("Node was null");
		    try {
		      return jsonMapper.writeValueAsString(jsonMapper.treeToValue(node, JsonNode.class));
		    } catch (Exception ex) {
		      throw new RuntimeException("Failed to write object to buffer", ex);
		    }
		  }

			/**
			 * Serializes the given object to a JSON byte array
			 * @param object The object to serialize
			 * @return A JSON formatted byte array
			 * @throws IllegalArgumentException if the object was null
			 * @throws JSONException if the object could not be serialized
			 */
			public static final byte[] serializeToBytes(final Object object) {
				if (object == null)
					throw new IllegalArgumentException("Object was null");
				try {
					return jsonMapper.writeValueAsBytes(object);
				} catch (JsonProcessingException e) {
					throw new JSONException(e);
				}
			}

			
		  /**
		   * Serializes the passed object to a string
		   * 
		   * @param object
		   *          The object to serialize
		   * @param serializer
		   *          The optional serializer to use
		   * @return the written string
		   */
		  public static <T> String serializeToString(final T object, final JsonSerializer<T> serializer) {
		    if (object == null)
		      throw new IllegalArgumentException("Object was null");
		    try {
		      if(serializer!=null) {
		        StringWriter sw = new StringWriter();
		        JsonGenerator gen = jsonFactory.createGenerator(sw);
		        try {
		          serializer.serialize(object, gen, serProvider);
		          gen.flush();
		          sw.flush();
		        } finally {
		          try { gen.close(); } catch (Exception x) {/* No Op */}
		        }
		        return sw.toString();
		      }
		      return jsonMapper.writeValueAsString(object);
		    } catch (Exception ex) {
		      throw new RuntimeException("Failed to write object to buffer", ex);
		    }
		  }
		  
		  /**
		   * Serializes the passed object to a string
		   * 
		   * @param object
		   *          The object to serialize
		   *          The buffer to write to, or null to create a new one
		   * @return the written string
		   */
		  public static String serializeToString(final Object object) {
		    return serializeToString(object, null);
		  }

	
	private JSONOps() {}

}
