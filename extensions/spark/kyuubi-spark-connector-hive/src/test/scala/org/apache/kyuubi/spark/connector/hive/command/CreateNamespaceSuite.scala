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

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.SupportsNamespaces
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.spark.connector.hive.command.DDLCommandTestUtils.{V1_COMMAND_VERSION, V2_COMMAND_VERSION}

trait CreateNamespaceSuiteBase extends DDLCommandTestUtils {
  override protected def command: String = "CREATE NAMESPACE"

  protected def namespaceArray: Array[String] = namespace.split('.')

  protected def notFoundMsgPrefix: String =
    if (commandVersion == V1_COMMAND_VERSION) "Database" else "Namespace"

  private def namespace: String = "fakens"

  override def afterEach(): Unit = {
    sql(s"DROP NAMESPACE IF EXISTS $catalogName.$namespace")
    super.afterEach()
  }

  test("basic test") {
    val ns = s"$catalogName.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      assert(getCatalog(catalogName).asNamespaceCatalog.namespaceExists(namespaceArray))
    }
  }

  test("namespace with location") {
    val ns = s"$catalogName.$namespace"
    withNamespace(ns) {
      withTempDir { tmpDir =>
        // The generated temp path is not qualified.
        val path = tmpDir.getCanonicalPath
        assert(!path.startsWith("file:/"))

        val e = intercept[IllegalArgumentException] {
          sql(s"CREATE NAMESPACE $ns LOCATION ''")
        }
        assert(e.getMessage.contains("Can not create a Path from an empty string") ||
          e.getMessage.contains("The location name cannot be empty string"))

        val uri = new Path(path).toUri
        sql(s"CREATE NAMESPACE $ns LOCATION '$uri'")

        // Make sure the location is qualified.
        val expected = makeQualifiedPath(tmpDir.toString)
        assert("file" === expected.getScheme)
        assert(new Path(getNamespaceLocation(catalogName, namespaceArray)).toUri === expected)
      }
    }
  }

  test("Namespace already exists") {
    val ns = s"$catalogName.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")

      val e = intercept[NamespaceAlreadyExistsException] {
        sql(s"CREATE NAMESPACE $ns")
      }
      assert(e.getMessage.contains(s"Namespace '$namespace' already exists") ||
        e.getMessage.contains(s"Cannot create schema `fakens` because it already exists"))

      // The following will be no-op since the namespace already exists.
      Try { sql(s"CREATE NAMESPACE IF NOT EXISTS $ns") }.isSuccess
    }
  }

  test("CreateNameSpace: reserved properties") {
    import SupportsNamespaces._
    val ns = s"$catalogName.$namespace"
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        val exception = intercept[ParseException] {
          sql(s"CREATE NAMESPACE $ns WITH DBPROPERTIES('$key'='dummyVal')")
        }
        assert(exception.getMessage.contains(s"$key is a reserved namespace property"))
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace(ns) {
          sql(s"CREATE NAMESPACE $ns WITH DBPROPERTIES('$key'='foo')")
          assert(
            sql(s"DESC NAMESPACE EXTENDED $ns")
              .toDF("k", "v")
              .where("k='Properties'")
              .where("v=''")
              .count == 1,
            s"$key is a reserved namespace property and ignored")
          val meta =
            getCatalog(catalogName).asNamespaceCatalog.loadNamespaceMetadata(namespaceArray)
          assert(
            meta.get(key) == null || !meta.get(key).contains("foo"),
            "reserved properties should not have side effects")
        }
      }
    }
  }

  protected def getNamespaceLocation(catalog: String, namespace: Array[String]): String = {
    val metadata = getCatalog(catalog).asNamespaceCatalog
      .loadNamespaceMetadata(namespace).asScala
    metadata(SupportsNamespaces.PROP_LOCATION)
  }
}

class CreateNamespaceV2Suite extends CreateNamespaceSuiteBase {

  override protected def catalogVersion: String = "Hive V2"

  override protected def commandVersion: String = V2_COMMAND_VERSION
}

class CreateNamespaceV1Suite extends CreateNamespaceSuiteBase {

  val SESSION_CATALOG_NAME: String = "spark_catalog"

  override protected val catalogName: String = SESSION_CATALOG_NAME

  override protected def catalogVersion: String = "V1"

  override protected def commandVersion: String = V1_COMMAND_VERSION
}
