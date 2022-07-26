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

public class InfoDetail {
  private String infoType;
  private String infoValue;

  public InfoDetail() {}

  public InfoDetail(String infoType, String infoValue) {
    this.infoType = infoType;
    this.infoValue = infoValue;
  }

  public String getInfoType() {
    return infoType;
  }

  public void setInfoType(String infoType) {
    this.infoType = infoType;
  }

  public String getInfoValue() {
    return infoValue;
  }

  public void setInfoValue(String infoValue) {
    this.infoValue = infoValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InfoDetail that = (InfoDetail) o;
    return Objects.equals(getInfoType(), that.getInfoType())
        && Objects.equals(getInfoValue(), that.getInfoValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getInfoType(), getInfoValue());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
