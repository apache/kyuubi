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

public class WebuiConfiguration {

  private long refreshInterval;

  private String timezoneName;

  private long timezoneOffset;

  private String kyuubiVersion;

  private String kyuubiRevision;

  public WebuiConfiguration() {}

  public WebuiConfiguration(
      long refreshInterval,
      String timezoneName,
      long timezoneOffset,
      String kyuubiVersion,
      String kyuubiRevision) {
    this.refreshInterval = refreshInterval;
    this.timezoneName = timezoneName;
    this.timezoneOffset = timezoneOffset;
    this.kyuubiVersion = kyuubiVersion;
    this.kyuubiRevision = kyuubiRevision;
  }

  public long getRefreshInterval() {
    return refreshInterval;
  }

  public String getTimezoneName() {
    return timezoneName;
  }

  public long getTimezoneOffset() {
    return timezoneOffset;
  }

  public String getKyuubiVersion() {
    return kyuubiVersion;
  }

  public String getKyuubiRevision() {
    return kyuubiRevision;
  }

  public void setRefreshInterval(long refreshInterval) {
    this.refreshInterval = refreshInterval;
  }

  public void setTimezoneName(String timezoneName) {
    this.timezoneName = timezoneName;
  }

  public void setTimezoneOffset(long timezoneOffset) {
    this.timezoneOffset = timezoneOffset;
  }

  public void setKyuubiVersion(String kyuubiVersion) {
    this.kyuubiVersion = kyuubiVersion;
  }

  public void setKyuubiRevision(String kyuubiRevision) {
    this.kyuubiRevision = kyuubiRevision;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WebuiConfiguration)) {
      return false;
    }
    WebuiConfiguration that = (WebuiConfiguration) o;
    return getRefreshInterval() == that.getRefreshInterval()
        && getTimezoneOffset() == that.getTimezoneOffset()
        && Objects.equals(getTimezoneName(), that.getTimezoneName())
        && getKyuubiVersion().equals(that.getKyuubiVersion())
        && Objects.equals(getKyuubiRevision(), that.getKyuubiRevision());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getRefreshInterval(),
        getTimezoneName(),
        getTimezoneOffset(),
        getKyuubiVersion(),
        getKyuubiRevision());
  }
}
