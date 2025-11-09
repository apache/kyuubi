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

import scala.concurrent.duration.DurationInt

import com.google.common.collect.Maps
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout

import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.HIVE_FILE_STATUS_CACHE_SCOPE
import org.apache.kyuubi.spark.connector.hive.read.HiveFileStatusCache

class HiveFileStatusCacheSuite extends KyuubiHiveTest {

  test("use different cache scope") {
    Seq("GLOBE", "NONE").foreach { value =>
      withSparkSession(Map(HIVE_FILE_STATUS_CACHE_SCOPE.key -> value)) { _ =>
        val path = new Path("/dummy_tmp", "abc")
        val files = (1 to 3).map(_ => new FileStatus())

        HiveFileStatusCache.resetForTesting()
        val fileStatusCacheTabel = HiveFileStatusCache.getOrCreate(spark, "catalog.db.catTable")
        fileStatusCacheTabel.putLeafFiles(path, files.toArray)

        value match {
          // Exactly 3 files are cached.
          case "GLOBE" =>
            assert(fileStatusCacheTabel.getLeafFiles(path).get.length === 3)
          case "NONE" =>
            assert(fileStatusCacheTabel.getLeafFiles(path).isEmpty)
          case _ =>
            throw new IllegalArgumentException(
              s"Unexpected value: '$value'. Only 'GLOBE' or 'NONE' are allowed.")
        }

        fileStatusCacheTabel.invalidateAll()
        assert(fileStatusCacheTabel.getLeafFiles(path).isEmpty)
      }
    }
  }

  test("cached by qualifiedName") {
    val previousValue = SQLConf.get.getConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS)
    try {
      // using 'SQLConf.get.setConf' instead of 'withSQLConf' to set a static config at runtime
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, 1L)

      val path = new Path("/dummy_tmp", "abc")
      val files = (1 to 3).map(_ => new FileStatus())

      HiveFileStatusCache.resetForTesting()
      val fileStatusCacheTabel1 = HiveFileStatusCache.getOrCreate(spark, "catalog.db.cat1Table")
      fileStatusCacheTabel1.putLeafFiles(path, files.toArray)
      val fileStatusCacheTabel2 = HiveFileStatusCache.getOrCreate(spark, "catalog.db.cat1Table")
      val fileStatusCacheTabel3 = HiveFileStatusCache.getOrCreate(spark, "catalog.db.table2")

      // Exactly 3 files are cached.
      assert(fileStatusCacheTabel1.getLeafFiles(path).get.length === 3)
      assert(fileStatusCacheTabel2.getLeafFiles(path).get.length === 3)
      assert(fileStatusCacheTabel3.getLeafFiles(path).isEmpty)
      // Wait until the cache expiration.
      eventually(timeout(3.seconds)) {
        // And the cache is gone.
        assert(fileStatusCacheTabel1.getLeafFiles(path).isEmpty)
        assert(fileStatusCacheTabel2.getLeafFiles(path).isEmpty)
        assert(fileStatusCacheTabel3.getLeafFiles(path).isEmpty)
      }
    } finally {
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, previousValue)
    }
  }

  test("expire FileStatusCache if TTL is configured") {
    val previousValue = SQLConf.get.getConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS)
    try {
      // using 'SQLConf.get.setConf' instead of 'withSQLConf' to set a static config at runtime
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, 1L)

      val path = new Path("/dummy_tmp", "abc")
      val files = (1 to 3).map(_ => new FileStatus())

      HiveFileStatusCache.resetForTesting()
      val fileStatusCache = HiveFileStatusCache.getOrCreate(spark, "catalog.db.table")
      fileStatusCache.putLeafFiles(path, files.toArray)

      // Exactly 3 files are cached.
      assert(fileStatusCache.getLeafFiles(path).get.length === 3)
      // Wait until the cache expiration.
      eventually(timeout(3.seconds)) {
        // And the cache is gone.
        assert(fileStatusCache.getLeafFiles(path).isEmpty)
      }
    } finally {
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, previousValue)
    }
  }

  private def newCatalog(): HiveTableCatalog = {
    val catalog = new HiveTableCatalog
    val properties = Maps.newHashMap[String, String]()
    properties.put("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:memorydb;create=true")
    properties.put("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    catalog.initialize(catalogName, new CaseInsensitiveStringMap(properties))
    catalog
  }

  test("expire FileStatusCache when insert into") {
    val dbName = "default"
    val tbName = "tbl_partition"
    val table = s"${catalogName}.${dbName}.${tbName}"

    withTable(table) {
      spark.sql(s"create table $table (age int)partitioned by(city string) stored as orc").collect()
      val location = newCatalog()
        .loadTable(Identifier.of(Array(dbName), tbName))
        .asInstanceOf[HiveTable]
        .catalogTable.location.toString

      spark.sql(s"insert into $table partition(city='ct') values(10),(20),(30),(40),(50)").collect()
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isEmpty)

      assert(spark.sql(s"select * from $table").count() === 5)
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isDefined)

      // should clear cache
      spark.sql(s"insert into $table partition(city='ct') values(11),(21),(31),(41),(51)").collect()
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isEmpty)

      assert(spark.sql(s"select * from $table").count() === 10)
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isDefined)
    }
  }

  test("expire FileStatusCache when insert overwrite") {
    val dbName = "default"
    val tbName = "tbl_partition"
    val table = s"${catalogName}.${dbName}.${tbName}"

    withTable(table) {
      spark.sql(s"create table $table (age int)partitioned by(city string) stored as orc").collect()
      val location = newCatalog()
        .loadTable(Identifier.of(Array(dbName), tbName))
        .asInstanceOf[HiveTable]
        .catalogTable.location.toString

      spark.sql(s"insert into $table partition(city='ct') values(10),(20),(30),(40),(50)").collect()
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isEmpty)

      assert(spark.sql(s"select * from $table").count() === 5)
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isDefined)

      // should clear cache
      spark.sql(s"insert overwrite $table partition(city='ct') values(11),(21),(31),(41),(51)")
        .collect()
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isEmpty)

      assert(spark.sql(s"select * from $table").count() === 5)
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isDefined)
    }
  }

  test("expire FileStatusCache when alter Table") {
    val dbName = "default"
    val tbName = "tbl_partition"
    val table = s"${catalogName}.${dbName}.${tbName}"

    withTable(table) {
      spark.sql(s"create table $table (age int)partitioned by(city string) stored as orc").collect()
      val location = newCatalog()
        .loadTable(Identifier.of(Array(dbName), tbName))
        .asInstanceOf[HiveTable]
        .catalogTable.location.toString

      spark.sql(s"insert into $table partition(city='ct') values(10),(20),(30),(40),(50)").collect()
      spark.sql(s"select * from $table").collect()
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isDefined)

      // should clear cache
      spark.sql(s"ALTER TABLE $table ADD COLUMNS (name string)").collect()
      assert(HiveFileStatusCache.getOrCreate(spark, table)
        .getLeafFiles(new Path(s"$location/city=ct")).isEmpty)
    }
  }

  test("expire FileStatusCache when rename Table") {
    val dbName = "default"
    val oldTbName = "tbl_partition"
    val newTbName = "tbl_partition_new"
    val oldTable = s"$catalogName.$dbName.$oldTbName"
    val newTable = s"$catalogName.$dbName.$newTbName"

    withTable(newTable) {
      spark.sql(s"create table ${oldTable} (age int)partitioned by(city string) stored as orc")
        .collect()
      spark.sql(s"insert into $oldTable partition(city='ct') values(10),(20),(30),(40),(50)")
        .collect()
      spark.sql(s"select * from $oldTable").collect()

      val oldLocation = newCatalog()
        .loadTable(Identifier.of(Array(dbName), oldTbName))
        .asInstanceOf[HiveTable]
        .catalogTable.location.toString
      assert(HiveFileStatusCache.getOrCreate(spark, oldTable)
        .getLeafFiles(new Path(s"$oldLocation/city=ct")).isDefined)

      spark.sql(s"DROP TABLE IF EXISTS ${newTable}").collect()
      spark.sql(s"use ${catalogName}.${dbName}").collect()
      spark.sql(s"ALTER TABLE $oldTbName RENAME TO $newTbName").collect()
      val newLocation = newCatalog()
        .loadTable(Identifier.of(Array(dbName), newTbName))
        .asInstanceOf[HiveTable]
        .catalogTable.location.toString

      assert(HiveFileStatusCache.getOrCreate(spark, oldTable)
        .getLeafFiles(new Path(s"$oldLocation/city=ct"))
        .isEmpty)

      assert(HiveFileStatusCache.getOrCreate(spark, newTable)
        .getLeafFiles(new Path(s"$newLocation/city=ct"))
        .isEmpty)
    }
  }

  test("FileStatusCache isolated between different catalogs with same database.table") {
    val catalog1 = catalogName
    val catalog2 = "hive2"
    val dbName = "default"
    val tbName = "tbl_partition"
    val cat1Table = s"${catalog1}.${dbName}.${tbName}"
    val cat2Table = s"${catalog2}.${dbName}.${tbName}"

    withTable(cat1Table, cat2Table) {
      spark.sql(s"CREATE TABLE IF NOT EXISTS $cat1Table (age int)partitioned by(city string)" +
        s" stored as orc").collect()
      spark.sql(s"CREATE TABLE IF NOT EXISTS $cat2Table (age int)partitioned by(city string)" +
        s" stored as orc").collect()

      val location = newCatalog()
        .loadTable(Identifier.of(Array(dbName), tbName))
        .asInstanceOf[HiveTable]
        .catalogTable.location.toString

      spark.sql(s"insert into $cat1Table partition(city='ct1') " +
        s"values(11),(12),(13),(14),(15)").collect()
      spark.sql(s"select * from $cat1Table where city='ct1'").collect()
      assert(HiveFileStatusCache.getOrCreate(spark, cat1Table)
        .getLeafFiles(new Path(s"$location/city=ct1")).isDefined)
      assert(HiveFileStatusCache.getOrCreate(spark, cat2Table)
        .getLeafFiles(new Path(s"$location/city=ct1")).isEmpty)

      spark.sql(s"insert into $cat2Table partition(city='ct2') " +
        s"values(21),(22),(23),(24),(25)").collect()
      spark.sql(s"select * from $cat2Table where city='ct2'").collect()
      assert(HiveFileStatusCache.getOrCreate(spark, cat1Table)
        .getLeafFiles(new Path(s"$location/city=ct2")).isEmpty)
      assert(HiveFileStatusCache.getOrCreate(spark, cat2Table)
        .getLeafFiles(new Path(s"$location/city=ct2")).isDefined)
    }
  }
}
