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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class GetBatchesResponse {
  private int from;
  private int total;
  private List<Batch> batches;

  public GetBatchesResponse() {}

  public GetBatchesResponse(int from, int total, List<Batch> batches) {
    this.from = from;
    this.total = total;
    this.batches = batches;
  }

  public int getFrom() {
    return from;
  }

  public void setFrom(int from) {
    this.from = from;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

  public List<Batch> getBatches() {
    if (null == batches) {
      return Collections.emptyList();
    }
    return batches;
  }

  public void setBatches(List<Batch> batches) {
    this.batches = batches;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetBatchesResponse that = (GetBatchesResponse) o;
    return getFrom() == that.getFrom()
        && getTotal() == that.getTotal()
        && Objects.equals(getBatches(), that.getBatches());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFrom(), getTotal(), getBatches());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
