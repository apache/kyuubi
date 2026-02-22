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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, PartitionsAlreadyExistException}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsPartitionManagement, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.spark.connector.hive.command.DDLCommandTestUtils.{V1_COMMAND_VERSION, V2_COMMAND_VERSION}

trait PartitionManagementSuite extends DDLCommandTestUtils {
  override protected def command: String = "PARTITION MANAGEMENT"

  test("create partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id string, year string, month string) PARTITIONED BY (year, month)")
      sql(s"ALTER TABLE $t ADD PARTITION (year='2023', month='01')")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Row("year=2023/month=01") :: Nil)
      intercept[PartitionsAlreadyExistException] {
        sql(s"ALTER TABLE $t ADD PARTITION (year='2023', month='01')")
      }
    }
  }

  test("drop partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id string, year string, month string) PARTITIONED BY (year, month)")
      sql(s"ALTER TABLE $t ADD PARTITION (year='2023', month='01')")
      sql(s"ALTER TABLE $t DROP PARTITION (year='2023', month='01')")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Nil)
      intercept[NoSuchPartitionsException] {
        sql(s"ALTER TABLE $t DROP PARTITION (year='9999', month='99')")
      }
    }
  }

  test("show partitions") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id string, year string, month string) PARTITIONED BY (year, month)")
      sql(s"ALTER TABLE $t ADD PARTITION (year='2023', month='01')")
      sql(s"ALTER TABLE $t ADD PARTITION (year='2023', month='02')")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Row("year=2023/month=01") :: Row("year=2023/month=02") :: Nil)

      checkAnswer(
        sql(s"SHOW PARTITIONS $t PARTITION (year='2023', month='01')"),
        Row("year=2023/month=01") :: Nil)

      checkAnswer(
        sql(s"SHOW PARTITIONS $t PARTITION (year='2023')"),
        Row("year=2023/month=01") :: Row("year=2023/month=02") :: Nil)
    }
  }
}

class PartitionManagementV2Suite extends PartitionManagementSuite {
  override protected def catalogVersion: String = "Hive V2"
  override protected def commandVersion: String = V2_COMMAND_VERSION

  test("create partition with location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id string, year string, month string) PARTITIONED BY (year, month)")
      val loc = "file:///tmp/kyuubi/hive_catalog_part_loc"
      sql(s"ALTER TABLE $t ADD PARTITION (year='2023', month='01') LOCATION '$loc'")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Row("year=2023/month=01") :: Nil)
      val catalog = spark.sessionState.catalogManager
        .catalog(catalogName).asInstanceOf[TableCatalog]
      val partManagement = catalog.loadTable(Identifier.of(Array("ns"), "tbl"))
        .asInstanceOf[SupportsPartitionManagement]
      val partIdent = InternalRow.fromSeq(
        Seq(UTF8String.fromString("2023"), UTF8String.fromString("01")))
      val metadata = partManagement.loadPartitionMetadata(partIdent)
      assert(metadata.containsKey("location"))
      assert(metadata.get("location").contains("hive_catalog_part_loc"))
    }
  }
}

class PartitionManagementV1Suite extends PartitionManagementSuite {
  val SESSION_CATALOG_NAME: String = "spark_catalog"
  override protected val catalogName: String = SESSION_CATALOG_NAME
  override protected def catalogVersion: String = "V1"
  override protected def commandVersion: String = V1_COMMAND_VERSION
}
