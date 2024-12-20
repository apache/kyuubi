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

package org.apache.kyuubi.spark.connector.yarn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class YarnLogQuerySuite extends SparkYarnConnectorWithYarn {
  test("query logs with host") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      // .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
    miniHdfsService.getHadoopConf.forEach(kv => {
      if (kv.getKey.startsWith("fs")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    miniYarnService.getYarnConf.forEach(kv => {
      if (kv.getKey.startsWith("yarn")) {
        sparkConf.set(s"spark.hadoop.${kv.getKey}", kv.getValue)
      }
    })
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE yarn")
      val cnt = spark.sql(
        "select count(1) from yarn.default.app_logs where host='localhost'").collect().head.getLong(
        0)
      assert(cnt > 0)
    }
  }

}
