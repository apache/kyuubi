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

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, PartitionsAlreadyExistException}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsPartitionManagement, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.spark.connector.hive.command.DDLCommandTestUtils.{V1_COMMAND_VERSION, V2_COMMAND_VERSION}

trait PartitionManagementSuite extends DDLCommandTestUtils {
  override protected def command: String = "PARTITION MANAGEMENT"

  test("drop partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(
        s"""CREATE TABLE $t (id string, name string, year int, dt date)
           |PARTITIONED BY (name, year, dt)""".stripMargin)
      sql(s"ALTER TABLE $t ADD PARTITION (name='a', year=2023, dt='2023-01-01')")
      sql(s"ALTER TABLE $t DROP PARTITION (name='a', year=2023, dt='2023-01-01')")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Nil)
      intercept[NoSuchPartitionsException] {
        sql(s"ALTER TABLE $t DROP PARTITION (name='a', year=9999, dt='9999-12-31')")
      }
    }
  }

  test("show partitions") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(
        s"""CREATE TABLE $t (id string, name string, year int, dt date)
           |PARTITIONED BY (name, year, dt)""".stripMargin)
      sql(s"ALTER TABLE $t ADD PARTITION (name='a', year=2023, dt='2023-01-01')")
      sql(s"ALTER TABLE $t ADD PARTITION (name='a', year=2023, dt='2023-02-01')")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Row("name=a/year=2023/dt=2023-01-01") ::
          Row("name=a/year=2023/dt=2023-02-01") :: Nil)

      checkAnswer(
        sql(s"SHOW PARTITIONS $t PARTITION (name='a', year=2023, dt='2023-01-01')"),
        Row("name=a/year=2023/dt=2023-01-01") :: Nil)

      checkAnswer(
        sql(s"SHOW PARTITIONS $t PARTITION (name='a', year=2023)"),
        Row("name=a/year=2023/dt=2023-01-01") ::
          Row("name=a/year=2023/dt=2023-02-01") :: Nil)
    }
  }
}

class PartitionManagementV2Suite extends PartitionManagementSuite {
  override protected def catalogVersion: String = "Hive V2"
  override protected def commandVersion: String = V2_COMMAND_VERSION

  test("create partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(
        s"""CREATE TABLE $t (id string, name string, year int, dt date)
           |PARTITIONED BY (name, year, dt)""".stripMargin)
      sql(s"ALTER TABLE $t ADD PARTITION (name='a', year=2023, dt='2023-01-01')")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Row("name=a/year=2023/dt=2023-01-01") :: Nil)
      intercept[PartitionsAlreadyExistException] {
        sql(s"ALTER TABLE $t ADD PARTITION (name='a', year=2023, dt='2023-01-01')")
      }
      sql(s"INSERT INTO $t PARTITION (name=null, year=null, dt=null) VALUES ('1')")
      checkAnswer(
        sql(s"SHOW PARTITIONS $t"),
        Row("name=a/year=2023/dt=2023-01-01") ::
          Row("name=null/year=null/dt=null") :: Nil)
      checkAnswer(
        sql(s"SELECT id, name, year, dt FROM $t WHERE name IS NULL"),
        Row("1", null, null, null) :: Nil)
    }
  }

  test("create partition with location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(
        s"""CREATE TABLE $t (id string, name string, year int, dt date)
           |PARTITIONED BY (name, year, dt)""".stripMargin)
      withTempDir { tmpDir =>
        val loc = new File(tmpDir, "hive_catalog_part_loc").toURI.toString
        sql(
          s"ALTER TABLE $t ADD PARTITION (name='a', year=2023, dt='2023-01-01') LOCATION '$loc'")
        checkAnswer(
          sql(s"SHOW PARTITIONS $t"),
          Row("name=a/year=2023/dt=2023-01-01") :: Nil)
        val catalog = spark.sessionState.catalogManager
          .catalog(catalogName).asInstanceOf[TableCatalog]
        val partManagement = catalog.loadTable(Identifier.of(Array("ns"), "tbl"))
          .asInstanceOf[SupportsPartitionManagement]
        val partIdent = InternalRow.fromSeq(
          Seq(
            UTF8String.fromString("a"),
            2023,
            java.sql.Date.valueOf("2023-01-01").toLocalDate.toEpochDay.toInt))
        val metadata = partManagement.loadPartitionMetadata(partIdent)
        assert(metadata.containsKey("location"))
        assert(metadata.get("location").contains("hive_catalog_part_loc"))
      }
    }
  }
}

class PartitionManagementV1Suite extends PartitionManagementSuite {
  val SESSION_CATALOG_NAME: String = "spark_catalog"
  override protected def catalogName: String = "spark_catalog"
  override protected def catalogVersion: String = "V1"
  override protected def commandVersion: String = V1_COMMAND_VERSION
}
