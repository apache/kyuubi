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

import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.QueryTest._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.util.JavaUtils

class YarnLogTableSuite extends KyuubiFunSuite {

  private val kyuubiHome: String =
    JavaUtils.getCodeSourceLocation(getClass).split("extensions").head

  private val remoteAppLogDir: Path = {
    val nioPath = Paths.get(kyuubiHome)
      .resolve("extensions")
      .resolve("spark")
      .resolve("kyuubi-spark-connector-yarn")
      .resolve("src")
      .resolve("test")
      .resolve("resources")
      .resolve("app-logs")
    new Path(nioPath.toUri)
  }

  test("read app logs") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
      .set("spark.hadoop.yarn.log-aggregation.file-formats", "TFile")
      .set("spark.hadoop.yarn.nodemanager.remote-app-log-dir", remoteAppLogDir.toString)
      .set("spark.hadoop.yarn.nodemanager.remote-app-log-dir-suffix", "logs")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      assert(spark.table("yarn.app_logs").count() === 5309)
    }
  }

  test("read app logs with filters") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.yarn", classOf[YarnCatalog].getName)
      .set("spark.hadoop.yarn.log-aggregation.file-formats", "TFile")
      .set("spark.hadoop.yarn.nodemanager.remote-app-log-dir", remoteAppLogDir.toString)
      .set("spark.hadoop.yarn.nodemanager.remote-app-log-dir-suffix", "logs")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      checkAnswer(
        spark.table("yarn.app_logs").groupBy("app_id").count().orderBy("app_id"),
        Seq(
          Row("application_1779560579873_0001", 1543),
          Row("application_1779560579873_0002", 1993),
          Row("application_1779560579873_0003", 1773)))
      assert(spark.table("yarn.app_logs")
        .filter("app_id='application_1779560579873_0001'").count() === 1543)
      assert(spark.table("yarn.app_logs")
        .filter("app_id='application_1779560579873_0002'").count() === 1993)
      assert(spark.table("yarn.app_logs")
        .filter("app_id='application_1779560579873_0003'").count() === 1773)

      checkAnswer(
        spark.table("yarn.app_logs").groupBy("user").count().orderBy("user"),
        Seq(
          Row("kyuubi", 1773),
          Row("root", 3536)))
      assert(spark.table("yarn.app_logs").filter("user='kyuubi'").count() === 1773)
      assert(spark.table("yarn.app_logs").filter("user='root'").count() === 3536)

      checkAnswer(
        spark.table("yarn.app_logs").groupBy("host").count().orderBy("host"),
        Seq(
          Row("hadoop-worker1.orb.local", 1532),
          Row("hadoop-worker2.orb.local", 2721),
          Row("hadoop-worker3.orb.local", 1056)))
      assert(spark.table("yarn.app_logs")
        .filter("host='hadoop-worker1.orb.local'").count() === 1532)
      assert(spark.table("yarn.app_logs")
        .filter("host='hadoop-worker2.orb.local'").count() === 2721)
      assert(spark.table("yarn.app_logs")
        .filter("host='hadoop-worker3.orb.local'").count() === 1056)

      checkAnswer(
        spark.table("yarn.app_logs").groupBy("container_id").count().orderBy("container_id"),
        Seq(
          Row("container_1779560579873_0001_01_000001", 619),
          Row("container_1779560579873_0001_01_000002", 462),
          Row("container_1779560579873_0001_01_000003", 462),
          Row("container_1779560579873_0002_01_000001", 799),
          Row("container_1779560579873_0002_01_000002", 600),
          Row("container_1779560579873_0002_01_000003", 594),
          Row("container_1779560579873_0003_01_000001", 848),
          Row("container_1779560579873_0003_01_000002", 455),
          Row("container_1779560579873_0003_01_000003", 470)))
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0001_01_000001'").count() === 619)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0001_01_000002'").count() === 462)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0001_01_000003'").count() === 462)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0002_01_000001'").count() === 799)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0002_01_000002'").count() === 600)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0002_01_000003'").count() === 594)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0003_01_000001'").count() === 848)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0003_01_000002'").count() === 455)
      assert(spark.table("yarn.app_logs")
        .filter("container_id='container_1779560579873_0003_01_000003'").count() === 470)

      checkAnswer(
        spark.table("yarn.app_logs").groupBy("log_type").count().orderBy("log_type"),
        Seq(
          Row("directory.info", 2991),
          Row("launch_container.sh", 492),
          Row("prelaunch.out", 36),
          Row("stderr", 9),
          Row("stdout", 1781)))
      assert(spark.table("yarn.app_logs").filter("log_type='stderr'").count() === 9)
      assert(spark.table("yarn.app_logs").filter("log_type='directory.info'").count() === 2991)
      assert(spark.table("yarn.app_logs").filter("log_type='launch_container.sh'").count() === 492)
      assert(spark.table("yarn.app_logs").filter("log_type='prelaunch.out'").count() === 36)
      assert(spark.table("yarn.app_logs").filter("log_type='stdout'").count() === 1781)
    }
  }
}
