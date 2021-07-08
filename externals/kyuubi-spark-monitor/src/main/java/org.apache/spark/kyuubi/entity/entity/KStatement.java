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

package org.apache.spark.kyuubi.entity.entity;

import java.util.HashMap;
import java.util.Map;

public class KStatement {

    private String statement;
    private String operationId;
    private String appId;
    private String sessionId;
    private Long executionId;
    private String physicPlan;
    private String logicalPlan;
    private Map<String, Long> stateTimeMap;

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getOperationId() {
        return operationId;
    }

    public void setOperationId(String operationId) {
        this.operationId = operationId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(Long executionId) {
        this.executionId = executionId;
    }

    public String getPhysicPlan() {
        return physicPlan;
    }

    public void setPhysicPlan(String physicPlan) {
        this.physicPlan = physicPlan;
    }

    public String getLogicalPlan() {
        return logicalPlan;
    }

    public void setLogicalPlan(String logicalPlan) {
        this.logicalPlan = logicalPlan;
    }

    public Map<String, Long> getStateTime() {
        return stateTimeMap;
    }

    public void setStateTime(Map<String, Long> stateTimeMap) {
        this.stateTimeMap.putAll(stateTimeMap);
    }

    public void setStateTime(String state, Long time) {
        this.stateTimeMap.put(state, time);
    }

    public KStatement() {
    }

    public KStatement(String statement, String operationId,
                      String appId, String sessionId,
                      String operationState, Long time) {
        this.statement = statement;
        this.operationId = operationId;
        this.appId = appId;
        this.sessionId = sessionId;

        this.stateTimeMap = new HashMap<String, Long>();
        stateTimeMap.put(operationState, time);
    }
}
