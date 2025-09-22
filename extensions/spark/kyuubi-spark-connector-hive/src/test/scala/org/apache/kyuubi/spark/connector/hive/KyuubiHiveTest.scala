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

package org.apache.kyuubi.spark.connector.hive

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.Utils

import org.apache.kyuubi.spark.connector.common.LocalSparkSession
import org.apache.kyuubi.spark.connector.hive.read.HiveFileStatusCache

abstract class KyuubiHiveTest extends QueryTest with Logging {

  private var innerSpark: SparkSession = _

  protected val TABLE_RESERVED_PROPERTIES =
    Seq(
      TableCatalog.PROP_COMMENT,
      TableCatalog.PROP_LOCATION,
      TableCatalog.PROP_PROVIDER,
      TableCatalog.PROP_OWNER,
      TableCatalog.PROP_EXTERNAL,
      TableCatalog.PROP_IS_MANAGED_LOCATION,
      "transient_lastDdlTime")

  protected val NAMESPACE_RESERVED_PROPERTIES =
    Seq(
      SupportsNamespaces.PROP_COMMENT,
      SupportsNamespaces.PROP_LOCATION,
      SupportsNamespaces.PROP_OWNER)

  protected val catalogName: String = "hive"

  override def beforeEach(): Unit = {
    super.beforeAll()
    HiveFileStatusCache.resetForTesting()
    getOrCreateSpark()
  }

  override def afterEach(): Unit = {
    super.afterAll()
    HiveFileStatusCache.resetForTesting()
    LocalSparkSession.stop(innerSpark)
  }

  def getOrCreateSpark(): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.catalog.hive", classOf[HiveTableCatalog].getName)
      .set("spark.sql.catalog.hive2", classOf[HiveTableCatalog].getName)
      .set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:memorydb;create=true")
      .set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")

    innerSpark = SparkSession.builder.config(sparkConf).getOrCreate()
  }

  def withSparkSession[T](conf: Map[String, String] = Map.empty[String, String])(
      f: SparkSession => T): T = {
    assert(innerSpark != null)
    conf.foreach {
      case (k, v) => innerSpark.sessionState.conf.setConfString(k, v)
    }
    f(innerSpark)
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  override def spark: SparkSession = innerSpark
}
