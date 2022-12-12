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

package org.apache.kyuubi.plugin.spark.authz.util

import org.apache.hadoop.conf.Configuration

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

trait RangerConfigProvider {

  /**
   * Get plugin config of different Ranger versions
   *
   * @return instance of
   *         org.apache.ranger.authorization.hadoop.config.RangerPluginConfig
   *         for Ranger 2.1 and above,
   *         or instance of
   *         org.apache.ranger.authorization.hadoop.config.RangerConfiguration
   *         for Ranger 2.0 and below
   */
  def getRangerConf: Configuration = {
    try {
      // for Ranger 2.1+
      invokeAs[Configuration](this, "getConfig")
    } catch {
      case _: NoSuchMethodException =>
        // for Ranger 2.0 and below
        invokeStatic(
          Class.forName("org.apache.ranger.authorization.hadoop.config.RangerConfiguration"),
          "getInstance")
          .asInstanceOf[Configuration]
    }
  }
}
