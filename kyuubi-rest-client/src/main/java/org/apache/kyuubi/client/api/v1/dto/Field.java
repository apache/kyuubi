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

import java.util.Base64;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Field {
  private String dataType;
  private Object value;

  public Field() {}

  public Field(String dataType, Object value) {
    this.dataType = dataType;
    this.value = value;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public Object getValue() {
    // For binary type column values, although the data type is "BINARY_VAL",
    // the value is transmitted as a Base64-encoded string.
    // Here, we decode it into a byte array.
    if (value instanceof String && "BINARY_VAL".equalsIgnoreCase(dataType)) {
      return Base64.getDecoder().decode((String) value);
    }
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Field field = (Field) o;
    return Objects.equals(getDataType(), field.getDataType())
        && Objects.equals(getValue(), field.getValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDataType(), getValue());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
