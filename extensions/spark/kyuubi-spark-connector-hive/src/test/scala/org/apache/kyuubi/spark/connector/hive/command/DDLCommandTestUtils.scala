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

package org.apache.kyuubi.spark.connector.hive.command

import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, SupportsNamespaces}
import org.scalactic.source
import org.scalatest.Tag

import org.apache.kyuubi.spark.connector.hive.{KyuubiHiveConnectorException, KyuubiHiveTest}

trait DDLCommandTestUtils extends KyuubiHiveTest {
  // The version of the catalog under testing such as "V1", "V2", "Hive V1", "Hive V2".
  protected def catalogVersion: String
  // The version of the SQL command under testing such as "V1", "V2".
  protected def commandVersion: String
  // Name of the command as SQL statement, for instance "SHOW PARTITIONS"
  protected def command: String

  // Overrides the `test` method, and adds a prefix to easily find identify the catalog to which
  // the failed test in logs belongs to.
  override def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */ )(implicit
      pos: source.Position): Unit = {
    val testNamePrefix = s"$command using $catalogVersion catalog $commandVersion command"
    super.test(s"$testNamePrefix: $testName", testTags: _*)(testFun)
  }

  // The metadata can not be distinguished between hive catalog and spark_catalog
  // because the catalogOption currently supported by spark is case-insensitive
  // So they share a metadata derby db.
  protected val builtinNamespace: Seq[String] = Seq("default")

  protected def withNamespace(namespaces: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      withSparkSession() { spark =>
        namespaces.foreach { name =>
          spark.sql(s"DROP NAMESPACE IF EXISTS $name CASCADE")
        }
      }
    }
  }

  protected def sql(sql: String): DataFrame = {
    withSparkSession() { spark =>
      spark.sql(sql)
    }
  }

  protected def getCatalog(name: String): CatalogPlugin = {
    withSparkSession() { spark =>
      spark.sessionState.catalogManager.catalog(name)
    }
  }

  protected def makeQualifiedPath(path: String): URI = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    fs.makeQualified(hadoopPath).toUri
  }

  protected def withNamespaceAndTable(
      ns: String,
      tableName: String,
      cat: String = catalogName)(f: String => Unit): Unit = {
    val nsCat = s"$cat.$ns"
    withNamespace(nsCat) {
      sql(s"CREATE NAMESPACE $nsCat")
      val t = s"$nsCat.$tableName"
      withTable(t) {
        f(t)
      }
    }
  }

  /**
   * Restores the current catalog/database after calling `f`.
   */
  protected def withCurrentCatalogAndNamespace(f: => Unit): Unit = {
    val curCatalog = sql("select current_catalog()").head().getString(0)
    val curDatabase = sql("select current_database()").head().getString(0)
    try {
      f
    } finally {
      spark.sql(s"USE $curCatalog.$curDatabase")
    }
  }

  implicit class CatalogHelper(plugin: CatalogPlugin) {
    def asNamespaceCatalog: SupportsNamespaces = plugin match {
      case namespaceCatalog: SupportsNamespaces =>
        namespaceCatalog
      case _ =>
        throw KyuubiHiveConnectorException(s"Catalog ${plugin.name} does not support namespaces")
    }
  }
}

object DDLCommandTestUtils {
  val V1_COMMAND_VERSION = "V1"
  val V2_COMMAND_VERSION = "V2"
}
