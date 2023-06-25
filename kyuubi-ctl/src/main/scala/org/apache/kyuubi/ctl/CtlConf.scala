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

package org.apache.kyuubi.ctl

import java.time.Duration

import org.apache.kyuubi.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.kyuubi.config.KyuubiConf.buildConf

object CtlConf {

  val CTL_REST_CLIENT_BASE_URL: OptionalConfigEntry[String] =
    buildConf("kyuubi.ctl.rest.base.url")
      .doc("The REST API base URL, " +
        "which contains the scheme (http:// or https://), hostname, port number")
      .version("1.6.0")
      .stringConf
      .createOptional

  val CTL_REST_CLIENT_AUTH_SCHEMA: ConfigEntry[String] =
    buildConf("kyuubi.ctl.rest.auth.schema")
      .doc("The authentication schema. Valid values are: basic, spnego.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("basic")

  val CTL_REST_CLIENT_SPNEGO_HOST: OptionalConfigEntry[String] =
    buildConf("kyuubi.ctl.rest.spnego.host")
      .doc("When auth schema is spnego, need to config spnego host.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val CTL_REST_CLIENT_CONNECT_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.ctl.rest.connect.timeout")
      .doc("The timeout[ms] for establishing the connection with the kyuubi server. " +
        "A timeout value of zero is interpreted as an infinite timeout.")
      .version("1.6.0")
      .timeConf
      .checkValue(_ >= 0, "must be 0 or positive number")
      .createWithDefault(Duration.ofSeconds(30).toMillis)

  val CTL_REST_CLIENT_SOCKET_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.ctl.rest.socket.timeout")
      .doc("The timeout[ms] for waiting for data packets after connection is established. " +
        "A timeout value of zero is interpreted as an infinite timeout.")
      .version("1.6.0")
      .timeConf
      .checkValue(_ >= 0, "must be 0 or positive number")
      .createWithDefault(Duration.ofSeconds(120).toMillis)

  val CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("kyuubi.ctl.rest.request.max.attempts")
      .doc("The max attempts number for ctl rest request.")
      .version("1.6.0")
      .intConf
      .createWithDefault(3)

  val CTL_REST_CLIENT_REQUEST_ATTEMPT_WAIT: ConfigEntry[Long] =
    buildConf("kyuubi.ctl.rest.request.attempt.wait")
      .doc("How long to wait between attempts of ctl rest request.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(3).toMillis)

  val CTL_BATCH_LOG_QUERY_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.ctl.batch.log.query.interval")
      .doc("The interval for fetching batch logs.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofSeconds(3).toMillis)

  val CTL_BATCH_LOG_ON_FAILURE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.ctl.batch.log.on.failure.timeout")
      .doc("The timeout for fetching remaining batch logs if the batch failed.")
      .version("1.6.1")
      .timeConf
      .createWithDefault(Duration.ofSeconds(10).toMillis)
}
