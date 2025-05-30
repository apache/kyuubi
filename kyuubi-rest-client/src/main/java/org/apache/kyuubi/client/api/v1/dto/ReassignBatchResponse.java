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

import java.util.*;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ReassignBatchResponse {
  private List<String> batchIds = new ArrayList<>(0);
  private String originalKyuubiInstance = null;
  private String newKyuubiInstance = null;

  public ReassignBatchResponse() {}

  public ReassignBatchResponse(
      List<String> batchIds, String originalKyuubiInstance, String newKyuubiInstance) {
    this.originalKyuubiInstance = originalKyuubiInstance;
    this.newKyuubiInstance = newKyuubiInstance;
    this.batchIds = batchIds;
  }

  public List<String> getBatchIds() {
    return batchIds;
  }

  public void setBatchIds(List<String> batchIds) {
    this.batchIds = batchIds;
  }

  public String getOriginalKyuubiInstance() {
    return originalKyuubiInstance;
  }

  public void setOriginalKyuubiInstance(String originalKyuubiInstance) {
    this.originalKyuubiInstance = originalKyuubiInstance;
  }

  public String getNewKyuubiInstance() {
    return newKyuubiInstance;
  }

  public void setNewKyuubiInstance(String newKyuubiInstance) {
    this.newKyuubiInstance = newKyuubiInstance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReassignBatchResponse that = (ReassignBatchResponse) o;
    return Objects.equals(getBatchIds(), that.getBatchIds())
        && Objects.equals(getOriginalKyuubiInstance(), that.getOriginalKyuubiInstance())
        && Objects.equals(getNewKyuubiInstance(), that.getNewKyuubiInstance());
  }

  @Override
  public int hashCode() {
    return Objects.hash(batchIds, originalKyuubiInstance, newKyuubiInstance);
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
