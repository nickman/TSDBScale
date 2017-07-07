/**
 * 
 */
package com.heliosapm.tsdbscale.core.config;

import org.springframework.context.annotation.Configuration;

/**
 * @author nwhitehead
 *
 */
@Configuration
public class PGConfiguration {
	/** The timescale postgres host name */
	private String host = "localhost";
	/** The timescale postgres listening port */
	private int port = 5432;
	/** The timescale postgres database name */
	private String database = "tscale";
	/** The timescale postgres user name */
	private String user = "tscale";
	/** The timescale postgres user password */
	private String password = "tscale";
	/** The timescale postgres connection pool size */
	private int poolSize = 20;
	
	/**
	 * Returns the connection pool max size
	 * @return the poolSize
	 */
	public int getPoolSize() {
		return poolSize;
	}
	
	/**
	 * Sets the connection pool max size
	 * @param poolSize the poolSize to set
	 */
	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}
	
	/**
	 * Returns the timescale postgres database host
	 * @return the host
	 */
	public String getHost() {
		return host;
	}
	
	/**
	 * Sets the timescale postgres database host
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}
	/**
	 * Returns the timescale postgres listening port
	 * @return the port
	 */
	public int getPort() {
		return port;
	}
	
	/**
	 * Sets the timescale postgres listening port
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}
	/**
	 * @return the database
	 */
	public String getDatabase() {
		return database;
	}
	/**
	 * @param database the database to set
	 */
	public void setDatabase(String database) {
		this.database = database;
	}
	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}
	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}
	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}
	
	
	
	
	
	
	
}
