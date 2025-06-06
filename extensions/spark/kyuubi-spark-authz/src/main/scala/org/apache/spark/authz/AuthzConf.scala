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
package org.apache.spark.authz

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf

object AuthzConf {
  val CONF_RESTRICTED_LIST =
    ConfigBuilder("spark.kyuubi.conf.restricted.list")
      .doc("The config key in the restricted list cannot set dynamic configuration via SET syntax.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val DATA_MASKING_ENABLED =
    buildConf("spark.sql.authz.dataMasking.enabled")
      .doc("Whether to enable data masking rule for authz plugin.")
      .version("1.11.0")
      .booleanConf
      .createWithDefault(true)

  val ROW_FILTER_ENABLED =
    buildConf("spark.sql.authz.rowFilter.enabled")
      .doc("Whether to enable row filter rule for authz plugin.")
      .version("1.11.0")
      .booleanConf
      .createWithDefault(true)

  def confRestrictedList(conf: SparkConf): Option[String] = {
    conf.get(CONF_RESTRICTED_LIST)
  }

  def dataMaskingEnabled(conf: SQLConf): Boolean = {
    conf.getConf(DATA_MASKING_ENABLED)
  }

  def rowFilterEnabled(conf: SQLConf): Boolean = {
    conf.getConf(ROW_FILTER_ENABLED)
  }
}
