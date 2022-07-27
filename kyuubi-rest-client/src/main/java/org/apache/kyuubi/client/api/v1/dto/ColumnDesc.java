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

public class ColumnDesc {
  private String columnName;
  private String dataType;
  private int columnIndex;
  private int precision;
  private int scale;
  private String comment;

  public ColumnDesc() {}

  public ColumnDesc(
      String columnName,
      String dataType,
      int columnIndex,
      int precision,
      int scale,
      String comment) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.columnIndex = columnIndex;
    this.precision = precision;
    this.scale = scale;
    this.comment = comment;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  public void setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnDesc that = (ColumnDesc) o;
    return getColumnIndex() == that.getColumnIndex()
        && getPrecision() == that.getPrecision()
        && getScale() == that.getScale()
        && Objects.equals(getColumnName(), that.getColumnName())
        && Objects.equals(getDataType(), that.getDataType())
        && Objects.equals(getComment(), that.getComment());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getColumnName(), getDataType(), getColumnIndex(), getPrecision(), getScale(), getComment());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
