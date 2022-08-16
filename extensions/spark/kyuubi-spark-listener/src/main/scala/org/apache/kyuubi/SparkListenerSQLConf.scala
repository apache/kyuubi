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

package org.apache.kyuubi

import org.apache.kyuubi.config.{ConfigBuilder, KyuubiConf}

object SparkListenerSQLConf {

  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val LINEAGE_EVENT_JSON_LOG_PATH =
    buildConf("spark.sql.lineage.event.json.log.path")
      .doc("The location of all the engine operation lineage events go for " +
        "the builtin JSON logger.<ul>" +
        "<li>Local Path: start with 'file://'</li>" +
        "<li>HDFS Path: start with 'hdfs://'</li></ul>")
      .version("1.6.0")
      .stringConf
      .createWithDefault("file:///tmp/kyuubi/events")

}
