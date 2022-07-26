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

public class GetColumnsRequest {
  private String catalogName;
  private String schemaName;
  private String tableName;
  private String columnName;

  public GetColumnsRequest() {}

  public GetColumnsRequest(
      String catalogName, String schemaName, String tableName, String columnName) {
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnName = columnName;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetColumnsRequest that = (GetColumnsRequest) o;
    return Objects.equals(getCatalogName(), that.getCatalogName())
        && Objects.equals(getSchemaName(), that.getSchemaName())
        && Objects.equals(getTableName(), that.getTableName())
        && Objects.equals(getColumnName(), that.getColumnName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCatalogName(), getSchemaName(), getTableName(), getColumnName());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
