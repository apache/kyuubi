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

package org.apache.kyuubi.plugin.spark.authz.ranger.datamasking
import java.sql.DriverManager

import scala.util.Try

import org.apache.spark.SparkConf
import org.scalatest.Outcome

import org.apache.kyuubi.plugin.spark.authz.V2JdbcTableCatalogPrivilegesBuilderSuite._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

class DataMaskingForJDBCV2Suite extends DataMaskingTestBase {
  override protected val extraSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.sql.defaultCatalog", "testcat")
      .set("spark.sql.catalog.testcat", v2JdbcTableCatalogClassName)
      .set("spark.sql.catalog.testcat.url", "jdbc:derby:memory:testcat;create=true")
      .set("spark.sql.catalog.testcat.driver", derbyJdbcDriverClass)
  }

  override protected val catalogImpl: String = "in-memory"

  override protected def format: String = ""

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // cleanup db
    Try {
      DriverManager.getConnection(s"jdbc:derby:memory:testcat;shutdown=true")
    }
  }

  override def withFixture(test: NoArgTest): Outcome = {
    test()
  }
}
