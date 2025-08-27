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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout

import org.apache.kyuubi.spark.connector.hive.read.HiveFileStatusCache

class HiveFileStatusCacheSuite extends KyuubiHiveTest {

  override def beforeEach(): Unit = {
    super.beforeEach()
    HiveFileStatusCache.resetForTesting()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    HiveFileStatusCache.resetForTesting()
  }

  test("cached by qualifiedName") {
    val previousValue = SQLConf.get.getConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS)
    try {
      // using 'SQLConf.get.setConf' instead of 'withSQLConf' to set a static config at runtime
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, 1L)

      val path = new Path("/dummy_tmp", "abc")
      val files = (1 to 3).map(_ => new FileStatus())

      HiveFileStatusCache.resetForTesting()
      val fileStatusCacheTabel1 = HiveFileStatusCache.getOrCreate(spark, "catalog.db.table1")
      fileStatusCacheTabel1.putLeafFiles(path, files.toArray)
      val fileStatusCacheTabel2 = HiveFileStatusCache.getOrCreate(spark, "catalog.db.table1")
      val fileStatusCacheTabel3 = HiveFileStatusCache.getOrCreate(spark, "catalog.db.table2")

      // Exactly 3 files are cached.
      assert(fileStatusCacheTabel1.getLeafFiles(path).get.length === 3)
      assert(fileStatusCacheTabel2.getLeafFiles(path).get.length === 3)
      assert(fileStatusCacheTabel3.getLeafFiles(path).isEmpty === true)
      // Wait until the cache expiration.
      eventually(timeout(3.seconds)) {
        // And the cache is gone.
        assert(fileStatusCacheTabel1.getLeafFiles(path).isEmpty === true)
        assert(fileStatusCacheTabel2.getLeafFiles(path).isEmpty === true)
        assert(fileStatusCacheTabel3.getLeafFiles(path).isEmpty === true)
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
        assert(fileStatusCache.getLeafFiles(path).isEmpty === true)
      }
    } finally {
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, previousValue)
    }
  }
}
