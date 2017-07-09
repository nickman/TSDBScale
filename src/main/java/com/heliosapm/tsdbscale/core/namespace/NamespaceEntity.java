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

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * <p>Title: NamespaceEntity</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbscale.core.namespace.NamespaceEntity</code></p>
 */
@Document(collection = "NamespaceEntries")
public class NamespaceEntity {
	@Id
	  private ObjectId id;

	  private String name;

	  private int version;

	  private ObjectId websiteId;

	  /**
	   * Using {@link Integer} and {@code null} to distinguish between updates
	   * and inserts. A {@code null} values is considered an insert and there is
	   * some code that auto generate value for it. A non {@code null} value is
	   * considered an update of an existing record.
	   *
	   * @see CounterEntity
	   */
	  private Integer resourceId;

	  /**
	   * The {@link Date} when this {@link Site} was created.
	   * NOTE: It's possibly {@code null}.
	   */
	  private Date createdAt;

	  /**
	   * The {@link Date} when this {@link Site} was updated.
	   * NOTE: It's possibly {@code null}.
	   */
	  private Date updatedAt;

	  @Transient
	  private boolean newId = false;

	  public ObjectId getId() {
	    return id;
	  }

	  public NamespaceEntity setId(ObjectId id) {
	    this.id = id;
	    return this;
	  }

	  public String getName() {
	    return name;
	  }

	  public NamespaceEntity setName(String name) {
	    this.name = name;
	    return this;
	  }

	  public int getVersion() {
	    return version;
	  }

	  public NamespaceEntity setVersion(int version) {
	    this.version = version;
	    return this;
	  }

	  public ObjectId getWebsiteId() {
	    return websiteId;
	  }

	  public NamespaceEntity setWebsiteId(ObjectId websiteId) {
	    this.websiteId = websiteId;
	    return this;
	  }

	  public Integer getResourceId() {
	    return resourceId;
	  }

	  public NamespaceEntity setResourceId(Integer resourceId) {
	    this.resourceId = resourceId;
	    return this;
	  }

	  public Date getCreatedAt() {
	    return createdAt;
	  }

	  public NamespaceEntity setCreatedAt(Date createdAt) {
	    this.createdAt = createdAt;
	    return this;
	  }

	  public Date getUpdatedAt() {
	    return updatedAt;
	  }

	  public NamespaceEntity setUpdatedAt(Date updatedAt) {
	    this.updatedAt = updatedAt;
	    return this;
	  }

	  public boolean isNewId() {
	    return newId;
	  }

	  public NamespaceEntity setNewId(boolean newId) {
	    this.newId = newId;
	    return this;
	  }
	  
}
