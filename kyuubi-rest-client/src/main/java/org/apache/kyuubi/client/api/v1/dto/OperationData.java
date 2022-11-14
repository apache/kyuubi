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

public class OperationData {
  private String identifier;
  private String OperationType;
  private String OperationStatus;

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getOperationType() {
    return OperationType;
  }

  public void setOperationType(String operationType) {
    OperationType = operationType;
  }

  public String getOperationStatus() {
    return OperationStatus;
  }

  public void setOperationStatus(String operationStatus) {
    OperationStatus = operationStatus;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OperationData that = (OperationData) o;
    return Objects.equals(identifier, that.identifier)
        && Objects.equals(OperationType, that.OperationType)
        && Objects.equals(OperationStatus, that.OperationStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier, OperationType, OperationStatus);
  }

  @Override
  public String toString() {
    return "OperationData{"
        + "identifier='"
        + identifier
        + '\''
        + ", OperationType='"
        + OperationType
        + '\''
        + ", OperationStatus='"
        + OperationStatus
        + '\''
        + '}';
  }
}
