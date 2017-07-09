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
package com.heliosapm.tsdbscale.core.namespace;

import java.util.Date;
import java.util.Objects;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>Title: Namespace</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.namespace.Namespace</code></p>
 */

public class Namespace {
	@JsonProperty
	@Null
	private String id;

	/**
	 * A chosen site identifier string (subdomain). If the name was already taken, we will attempt to add a suffix
	 */
	@JsonProperty
	@NotEmpty
	private String name;

	/**
	 * Platform version
	 */
	@JsonProperty
	@NotNull
	private int version;

	/**
	 * The website that this namespace should be applied to. The format will be validated against org.bson.types.ObjectId
	 */
	@JsonProperty
	private String websiteId;

	/**
	 * An immutable unique identifier used in a similar capacity as name which is unique but mutable
	 */
	@JsonProperty
	@Null
	private String resourceId;

	@JsonProperty
	@Null
	private Date createdAt;

	@JsonProperty
	@Null
	private Date updatedAt;

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public int getVersion() {
		return version;
	}

	public String getWebsiteId() {
		return websiteId;
	}

	public String getResourceId() {
		return resourceId;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setId(String id) {
		this.id =  id;
	}

	public void setName(String name) {
		this.name =  name;
	}

	public void setVersion(int version) {
		this.version =  version;
	}

	public void setWebsiteId(String websiteId) {
		this.websiteId =  websiteId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId =  resourceId;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt =  createdAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt =  updatedAt;
	}

	public Namespace withId(String id) {
		this.id =  id;
		return this;
	}

	public Namespace withName(String name) {
		this.name =  name;
		return this;
	}

	public Namespace withVersion(int version) {
		this.version =  version;
		return this;
	}

	public Namespace withWebsiteId(String websiteId) {
		this.websiteId =  websiteId;
		return this;
	}

	public Namespace withResourceId(String resourceId) {
		this.resourceId =  resourceId;
		return this;
	}

	public Namespace withCreatedAt(Date createdAt) {
		this.createdAt =  createdAt;
		return this;
	}

	public Namespace withUpdatedAt(Date updatedAt) {
		this.updatedAt =  updatedAt;
		return this;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append("id:").append(this.id).append(",");
		sb.append("name:").append(this.name).append(",");
		sb.append("version:").append(this.version).append(",");
		sb.append("websiteId:").append(this.websiteId).append(",");
		sb.append("resourceId:").append(this.resourceId).append(",");
		sb.append("createdAt:").append(this.createdAt).append(",");
		sb.append("updatedAt:").append(this.updatedAt);
		sb.append("}");
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Namespace that = (Namespace) o;
		return Objects.equals(id, that.id) &&
				Objects.equals(name, that.name) &&
				Objects.equals(version, that.version) &&
				Objects.equals(websiteId, that.websiteId) &&
				Objects.equals(resourceId, that.resourceId) &&
				Objects.equals(createdAt, that.createdAt) &&
				Objects.equals(updatedAt, that.updatedAt);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, name, version, websiteId, resourceId, createdAt, updatedAt);
	}
}
