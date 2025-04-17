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

public class KyuubiSessionEvent {

  private String sessionId;

  private int clientVersion;

  private String sessionType;

  private String sessionName;

  private String remoteSessionId;

  private String engineId;

  private String engineName;

  private String engineUrl;

  private String user;

  private String clientIp;

  private String serverIp;

  private Map<String, String> conf;

  private Map<String, String> optimizedConf;

  private long eventTime;

  private long openedTime;

  private long startTime;

  private long endTime;

  private int totalOperations;

  private Throwable exception;

  public KyuubiSessionEvent() {}

  public KyuubiSessionEvent(
      String sessionId,
      int clientVersion,
      String sessionType,
      String sessionName,
      String remoteSessionId,
      String engineId,
      String engineName,
      String engineUrl,
      String user,
      String clientIp,
      String serverIp,
      Map<String, String> conf,
      Map<String, String> optimizedConf,
      long eventTime,
      long openedTime,
      long startTime,
      long endTime,
      int totalOperations,
      Throwable exception) {
    this.sessionId = sessionId;
    this.clientVersion = clientVersion;
    this.sessionType = sessionType;
    this.sessionName = sessionName;
    this.remoteSessionId = remoteSessionId;
    this.engineId = engineId;
    this.engineName = engineName;
    this.engineUrl = engineUrl;
    this.user = user;
    this.clientIp = clientIp;
    this.serverIp = serverIp;
    this.conf = conf;
    this.optimizedConf = optimizedConf;
    this.eventTime = eventTime;
    this.openedTime = openedTime;
    this.startTime = startTime;
    this.endTime = endTime;
    this.totalOperations = totalOperations;
    this.exception = exception;
  }

  public static KyuubiSessionEvent.KyuubiSessionEventBuilder builder() {
    return new KyuubiSessionEvent.KyuubiSessionEventBuilder();
  }

  public static class KyuubiSessionEventBuilder {
    private String sessionId;

    private int clientVersion;

    private String sessionType;

    private String sessionName;

    private String remoteSessionId;

    private String engineId;

    private String engineName;

    private String engineUrl;

    private String user;

    private String clientIp;

    private String serverIp;

    private Map<String, String> conf;

    private Map<String, String> optimizedConf;

    private long eventTime;

    private long openedTime;

    private long startTime;

    private long endTime;

    private int totalOperations;

    private Throwable exception;

    public KyuubiSessionEventBuilder() {}

    public KyuubiSessionEvent.KyuubiSessionEventBuilder sessionId(final String sessionId) {
      this.sessionId = sessionId;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder remoteSessionId(
        final String remoteSessionId) {
      this.remoteSessionId = remoteSessionId;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder clientVersion(final int clientVersion) {
      this.clientVersion = clientVersion;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder sessionType(final String sessionType) {
      this.sessionType = sessionType;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder sessionName(final String sessionName) {
      this.sessionName = sessionName;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder engineId(final String engineId) {
      this.engineId = engineId;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder engineName(final String engineName) {
      this.engineName = engineName;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder engineUrl(final String engineUrl) {
      this.engineUrl = engineUrl;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder user(final String user) {
      this.user = user;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder clientIp(final String clientIp) {
      this.clientIp = clientIp;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder serverIp(final String serverIp) {
      this.serverIp = serverIp;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder conf(final Map<String, String> conf) {
      this.conf = conf;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder optimizedConf(
        final Map<String, String> optimizedConf) {
      this.optimizedConf = optimizedConf;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder eventTime(final long eventTime) {
      this.eventTime = eventTime;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder openedTime(final long openedTime) {
      this.openedTime = openedTime;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder startTime(final long startTime) {
      this.startTime = startTime;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder endTime(final long endTime) {
      this.endTime = endTime;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder totalOperations(final int totalOperations) {
      this.totalOperations = totalOperations;
      return this;
    }

    public KyuubiSessionEvent.KyuubiSessionEventBuilder exception(final Throwable exception) {
      this.exception = exception;
      return this;
    }

    public KyuubiSessionEvent build() {
      return new KyuubiSessionEvent(
          sessionId,
          clientVersion,
          sessionType,
          sessionName,
          remoteSessionId,
          engineId,
          engineName,
          engineUrl,
          user,
          clientIp,
          serverIp,
          conf,
          optimizedConf,
          eventTime,
          openedTime,
          startTime,
          endTime,
          totalOperations,
          exception);
    }
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public int getClientVersion() {
    return clientVersion;
  }

  public void setClientVersion(int clientVersion) {
    this.clientVersion = clientVersion;
  }

  public String getSessionType() {
    return sessionType;
  }

  public void setSessionType(String sessionType) {
    this.sessionType = sessionType;
  }

  public String getSessionName() {
    return sessionName;
  }

  public void setSessionName(String sessionName) {
    this.sessionName = sessionName;
  }

  public String getRemoteSessionId() {
    return remoteSessionId;
  }

  public void setRemoteSessionId(String remoteSessionId) {
    this.remoteSessionId = remoteSessionId;
  }

  public String getEngineId() {
    return engineId;
  }

  public void setEngineId(String engineId) {
    this.engineId = engineId;
  }

  public String getEngineName() {
    return engineName;
  }

  public void setEngineName(String engineName) {
    this.engineName = engineName;
  }

  public String getEngineUrl() {
    return engineUrl;
  }

  public void setEngineUrl(String engineUrl) {
    this.engineUrl = engineUrl;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getClientIp() {
    return clientIp;
  }

  public void setClientIp(String clientIp) {
    this.clientIp = clientIp;
  }

  public String getServerIp() {
    return serverIp;
  }

  public void setServerIp(String serverIp) {
    this.serverIp = serverIp;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public void setConf(Map<String, String> conf) {
    this.conf = conf;
  }

  public Map<String, String> getOptimizedConf() {
    return optimizedConf;
  }

  public void setOptimizedConf(Map<String, String> optimizedConf) {
    this.optimizedConf = optimizedConf;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

  public long getOpenedTime() {
    return openedTime;
  }

  public void setOpenedTime(long openedTime) {
    this.openedTime = openedTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public int getTotalOperations() {
    return totalOperations;
  }

  public void setTotalOperations(int totalOperations) {
    this.totalOperations = totalOperations;
  }

  public Throwable getException() {
    return exception;
  }

  public void setException(Throwable exception) {
    this.exception = exception;
  }
}
