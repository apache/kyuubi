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

import java.util.Map;

public class Batch {
  private String id;
  private String batchType;
  private Map<String, String> batchInfo;
  private String kyuubiInstance;
  private String state;

  public Batch() {}

  public Batch(
      String id,
      String batchType,
      Map<String, String> batchInfo,
      String kyuubiInstance,
      String state) {
    this.id = id;
    this.batchType = batchType;
    this.batchInfo = batchInfo;
    this.kyuubiInstance = kyuubiInstance;
    this.state = state;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getBatchType() {
    return batchType;
  }

  public void setBatchType(String batchType) {
    this.batchType = batchType;
  }

  public Map<String, String> getBatchInfo() {
    return batchInfo;
  }

  public void setBatchInfo(Map<String, String> batchInfo) {
    this.batchInfo = batchInfo;
  }

  public String getKyuubiInstance() {
    return kyuubiInstance;
  }

  public void setKyuubiInstance(String kyuubiInstance) {
    this.kyuubiInstance = kyuubiInstance;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }
}
