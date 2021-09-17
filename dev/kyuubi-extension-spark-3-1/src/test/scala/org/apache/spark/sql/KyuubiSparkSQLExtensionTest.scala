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
package org.apache.spark.sql

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

trait KyuubiSparkSQLExtensionTest extends QueryTest
    with SQLTestUtils
    with AdaptiveSparkPlanHelper {
  private var _spark: Option[SparkSession] = None
  protected def spark: SparkSession = _spark.getOrElse{
    throw new RuntimeException("test spark session don't initial before using it.")
  }

  protected override def beforeAll(): Unit = {
    if (_spark.isEmpty) {
      _spark = Option(SparkSession.builder()
        .master("local[1]")
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate())
      setupData()
      super.beforeAll()
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    cleanupData()
    _spark.foreach(_.stop)
  }

  private def setupData(): Unit = {
    val self = spark
    import self.implicits._
    spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString)), 10)
      .toDF("c1", "c2").createOrReplaceTempView("t1")
    spark.sparkContext.parallelize(
      (1 to 10).map(i => TestData(i, i.toString)), 5)
      .toDF("c1", "c2").createOrReplaceTempView("t2")
    spark.sparkContext.parallelize(
      (1 to 50).map(i => TestData(i, i.toString)), 2)
      .toDF("c1", "c2").createOrReplaceTempView("t3")
  }

  private def cleanupData(): Unit = {
    spark.sql("DROP VIEW IF EXISTS t1")
    spark.sql("DROP VIEW IF EXISTS t2")
    spark.sql("DROP VIEW IF EXISTS t3")
  }

  def sparkConf(): SparkConf = {
    val basePath = Utils.createTempDir() + getClass.getCanonicalName
    val metastorePath = basePath + "/metastore_db"
    val warehousePath = basePath + "/warehouse"
    new SparkConf()
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLExtension")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.hadoop.hive.metastore.client.capability.check", "false")
      .set(ConfVars.METASTORECONNECTURLKEY.varname,
        s"jdbc:derby:;databaseName=$metastorePath;create=true")
      .set(StaticSQLConf.WAREHOUSE_PATH, warehousePath)
      .set("spark.ui.enabled", "false")
  }
}
