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

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkSessionExtensions}
import org.scalatest.Assertions._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.V2JdbcTableCatalogPrivilegesBuilderSuite._
import org.apache.kyuubi.plugin.spark.authz.ranger.DeltaCatalogRangerSparkExtensionSuite._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

trait SparkSessionProvider {
  protected val catalogImpl: String
  protected def format: String = if (catalogImpl == "hive") "hive" else "parquet"

  protected val extension: SparkSessionExtensions => Unit = _ => ()
  protected val sqlExtensions: String = ""

  protected val extraSparkConf: SparkConf = new SparkConf()

  protected lazy val spark: SparkSession = {
    val ret = SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", "false")
      // 使用一个基础的内存数据库作为模板
      .config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:template_db;create=true")
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

  protected def doAs[T](user: String, f: => T): T = {
    UserGroupInformation.createRemoteUser(user).doAs[T](
      new PrivilegedExceptionAction[T] {
        override def run(): T = f
      })
  }
  protected def withCleanTmpResources[T](res: Seq[(String, String)])(f: => T): T = {
    try {
      f
    } finally {
      res.foreach {
        case (t, "table") => doAs(
            admin, {
              val purgeOption =
                if (isSparkV32OrGreater && isCatalogSupportPurge(
                    spark.sessionState.catalogManager.currentCatalog.name())) {
                  "PURGE"
                } else ""
              sql(s"DROP TABLE IF EXISTS $t $purgeOption")
            })
        case (db, "database") => doAs(admin, sql(s"DROP DATABASE IF EXISTS $db"))
        case (fn, "function") => doAs(admin, sql(s"DROP FUNCTION IF EXISTS $fn"))
        case (view, "view") => doAs(admin, sql(s"DROP VIEW IF EXISTS $view"))
        case (cacheTable, "cache") => if (isSparkV32OrGreater) {
            doAs(admin, sql(s"UNCACHE TABLE IF EXISTS $cacheTable"))
          }
        case (_, e) =>
          throw new RuntimeException(s"the resource whose resource type is $e cannot be cleared")
      }
    }
  }

  protected def checkAnswer(user: String, query: String, result: Seq[Row]): Unit = {
    doAs(user, assert(sql(query).collect() === result))
  }

  private def isCatalogSupportPurge(catalogName: String): Boolean = {
    val unsupportedCatalogs = Set(v2JdbcTableCatalogClassName, deltaCatalogClassName)
    spark.conf.getOption(s"spark.sql.catalog.$catalogName") match {
      case Some(catalog) if !unsupportedCatalogs.contains(catalog) => true
      case _ => false
    }
  }
}
