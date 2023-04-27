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

public class ExecPoolStatistic {
  private int execPoolSize;
  private int execPoolActiveCount;
  private int execPoolWorkQueueSize;

  public ExecPoolStatistic() {}

  public ExecPoolStatistic(int execPoolSize, int execPoolActiveCount, int execPoolWorkQueueSize) {
    this.execPoolSize = execPoolSize;
    this.execPoolActiveCount = execPoolActiveCount;
    this.execPoolWorkQueueSize = execPoolWorkQueueSize;
  }

  public int getExecPoolSize() {
    return execPoolSize;
  }

  public void setExecPoolSize(int execPoolSize) {
    this.execPoolSize = execPoolSize;
  }

  public int getExecPoolActiveCount() {
    return execPoolActiveCount;
  }

  public void setExecPoolActiveCount(int execPoolActiveCount) {
    this.execPoolActiveCount = execPoolActiveCount;
  }

  public int getExecPoolWorkQueueSize() {
    return execPoolWorkQueueSize;
  }

  public void setExecPoolWorkQueueSize(int execPoolWorkQueueSize) {
    this.execPoolWorkQueueSize = execPoolWorkQueueSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExecPoolStatistic that = (ExecPoolStatistic) o;
    return getExecPoolSize() == that.getExecPoolSize()
        && getExecPoolActiveCount() == that.getExecPoolActiveCount()
        && getExecPoolWorkQueueSize() == that.getExecPoolWorkQueueSize();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getExecPoolSize(), getExecPoolActiveCount(), getExecPoolWorkQueueSize());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
