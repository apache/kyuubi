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

package org.apache.kyuubi.plugin.spark.authz

import java.nio.file.Files
import java.security.PrivilegedExceptionAction

import scala.util.Try

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}
import org.scalatest.Assertions.{assertResult, intercept, withClue}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

trait SparkSessionProvider {
  protected val catalogImpl: String
  protected def format: String = if (catalogImpl == "hive") "hive" else "parquet"
  protected val isSparkV2: Boolean = isSparkVersionAtMost("2.4")
  protected val isSparkV31OrGreater: Boolean = isSparkVersionAtLeast("3.1")
  protected val isSparkV32OrGreater: Boolean = isSparkVersionAtLeast("3.2")
  protected val isSparkV33OrGreater: Boolean = isSparkVersionAtLeast("3.3")

  protected val extension: SparkSessionExtensions => Unit = _ => Unit
  protected val sqlExtensions: String = ""

  protected val defaultTableOwner = "default_table_owner"
  protected val extraSparkConf: SparkConf = new SparkConf()

  protected lazy val spark: SparkSession = {
    val metastore = {
      val path = Utils.createTempDir(prefix = "hms")
      Files.delete(path)
      path
    }
    val ret = SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", "false")
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastore;create=true")
      .config("spark.sql.catalogImplementation", catalogImpl)
      .config(
        "spark.sql.warehouse.dir",
        Utils.createTempDir("spark-warehouse").toString)
      .config("spark.sql.extensions", sqlExtensions)
      .withExtensions(extension)
      .config(extraSparkConf)
      .getOrCreate()
    if (catalogImpl == "hive") {
      // Ensure HiveExternalCatalog.client.userName is defaultTableOwner
      UserGroupInformation.createRemoteUser(defaultTableOwner).doAs(
        new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = ret.catalog.listDatabases()
        })
    }
    ret
  }

  protected val sql: String => DataFrame = spark.sql

  protected def doAs[T](user: String)(f: => T): T = {
    UserGroupInformation.createRemoteUser(user).doAs[T](
      new PrivilegedExceptionAction[T] {
        override def run(): T = f
      })
  }

  protected def runSqlAs(user: String)(
      sqlStr: String,
      isCollect: Boolean = false,
      collectSize: Int = Int.MinValue,
      countSize: Int = Int.MinValue): Unit = {
    doAs(user) {
      assert(Try {
        val df = sql(sqlStr)
        val collected = if (isCollect || collectSize >= 0) {
          df.collect()
        } else {
          Array.emptyObjectArray
        }
        if (collectSize >= 0) {
          assertResult(collectSize)(collected.length)
        }
        if (countSize >= 0) {
          assertResult(countSize)(df.count)
        }
      }.isSuccess)
    }
  }

  /**
   * intercept [AccessControlException] when running sql
   * and assert error message contains certain char sequence
   * @param user user name to execute sql , default to "someone"
   * @param sqlText parse ans run sql to dataframe
   * @param contains assert error message containing certain char sequence
   * @param isExplain whether execute explain for dataframe
   * @param isCollect whether execute collect for dataframe
   */
  protected def runSqlAsWithAccessException(user: String = "someone")(
      sqlText: String,
      contains: String,
      isExplain: Boolean = false,
      isCollect: Boolean = false): Unit = {
    withClue(sqlText) {
      val e = intercept[AccessControlException](
        doAs(user) {
          val df = sql(sqlText)
          if (isExplain) {
            df.explain()
          }
          if (isCollect) {
            df.collect()
          }
        })
      if (StringUtils.isNotBlank(contains)) {
        assert(e.getMessage.contains(contains))
      }
    }
  }

  protected def withCleanTmpResources[T](res: Seq[(String, String)])(f: => T): T = {
    try {
      f
    } finally {
      res.foreach {
        case (t, "table") => runSqlAs("admin")(s"DROP TABLE IF EXISTS $t")
        case (db, "database") => runSqlAs("admin")(s"DROP DATABASE IF EXISTS $db")
        case (fn, "function") => runSqlAs("admin")(s"DROP FUNCTION IF EXISTS $fn")
        case (view, "view") => runSqlAs("admin")(s"DROP VIEW IF EXISTS $view")
        case (cacheTable, "cache") => if (isSparkV32OrGreater) {
            runSqlAs("admin")(s"UNCACHE TABLE IF EXISTS $cacheTable")
          }
        case (_, e) =>
          throw new RuntimeException(s"the resource whose resource type is $e cannot be cleared")
      }
    }
  }

}
