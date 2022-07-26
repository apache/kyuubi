/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.client.api.v1.dto;

import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class GetCrossReferenceRequest {
  private String primaryCatalog;
  private String primarySchema;
  private String primaryTable;
  private String foreignCatalog;
  private String foreignSchema;
  private String foreignTable;

  public GetCrossReferenceRequest() {}

  public GetCrossReferenceRequest(
      String primaryCatalog,
      String primarySchema,
      String primaryTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    this.primaryCatalog = primaryCatalog;
    this.primarySchema = primarySchema;
    this.primaryTable = primaryTable;
    this.foreignCatalog = foreignCatalog;
    this.foreignSchema = foreignSchema;
    this.foreignTable = foreignTable;
  }

  public String getPrimaryCatalog() {
    return primaryCatalog;
  }

  public void setPrimaryCatalog(String primaryCatalog) {
    this.primaryCatalog = primaryCatalog;
  }

  public String getPrimarySchema() {
    return primarySchema;
  }

  public void setPrimarySchema(String primarySchema) {
    this.primarySchema = primarySchema;
  }

  public String getPrimaryTable() {
    return primaryTable;
  }

  public void setPrimaryTable(String primaryTable) {
    this.primaryTable = primaryTable;
  }

  public String getForeignCatalog() {
    return foreignCatalog;
  }

  public void setForeignCatalog(String foreignCatalog) {
    this.foreignCatalog = foreignCatalog;
  }

  public String getForeignSchema() {
    return foreignSchema;
  }

  public void setForeignSchema(String foreignSchema) {
    this.foreignSchema = foreignSchema;
  }

  public String getForeignTable() {
    return foreignTable;
  }

  public void setForeignTable(String foreignTable) {
    this.foreignTable = foreignTable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetCrossReferenceRequest that = (GetCrossReferenceRequest) o;
    return Objects.equals(getPrimaryCatalog(), that.getPrimaryCatalog())
        && Objects.equals(getPrimarySchema(), that.getPrimarySchema())
        && Objects.equals(getPrimaryTable(), that.getPrimaryTable())
        && Objects.equals(getForeignCatalog(), that.getForeignCatalog())
        && Objects.equals(getForeignSchema(), that.getForeignSchema())
        && Objects.equals(getForeignTable(), that.getForeignTable());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getPrimaryCatalog(),
        getPrimarySchema(),
        getPrimaryTable(),
        getForeignCatalog(),
        getForeignSchema(),
        getForeignTable());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
