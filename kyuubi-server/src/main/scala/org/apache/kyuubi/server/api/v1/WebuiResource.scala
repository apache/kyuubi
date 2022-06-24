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

package org.apache.kyuubi.server.api.v1

import java.time.ZonedDateTime
import java.time.format.TextStyle
import java.util.Locale
import javax.ws.rs.{GET, Path, Produces}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.{KYUUBI_VERSION, Logging, REVISION}
import org.apache.kyuubi.client.api.v1.dto.{OverviewInformation, VersionInfo, WebuiConfiguration}
import org.apache.kyuubi.server.api.ApiRequestContext

@Tag(name = "webui")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class WebuiResource extends ApiRequestContext with Logging {

  private def sessionManager = fe.be.sessionManager

  private val TIME_ZONE =
    ZonedDateTime.now.getZone.getDisplayName(TextStyle.FULL, Locale.getDefault)

  private val DEFAULT_REFRESH_INTERVAL = 10000L

  @GET
  @Path("configuration")
  def configuration(): WebuiConfiguration = {
    new WebuiConfiguration(
      DEFAULT_REFRESH_INTERVAL,
      TIME_ZONE,
      new VersionInfo(KYUUBI_VERSION).getVersion,
      REVISION)
  }

  @GET
  @Path("overview")
  def overview(): OverviewInformation = {
    new OverviewInformation(
      sessionManager.getOpenSessionCount,
      sessionManager.getExecPoolSize,
      sessionManager.getActiveCount)
  }

}
