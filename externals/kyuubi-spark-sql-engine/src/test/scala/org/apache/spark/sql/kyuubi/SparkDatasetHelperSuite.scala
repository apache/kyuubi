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

package org.apache.spark.sql.kyuubi

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import org.apache.arrow.compression.CommonsCompressionFactory
import org.apache.arrow.flatbuf.CompressionType
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

class SparkDatasetHelperSuite extends WithSparkSQLEngine {
  override def withKyuubiConf: Map[String, String] = Map.empty

  test("get limit from spark plan") {
    Seq(true, false).foreach { aqe =>
      val topKThreshold = 3
      spark.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, aqe)
      spark.sessionState.conf.setConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD, topKThreshold)
      spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS" +
        " SELECT * FROM VALUES(1),(2),(3),(4) AS t(id)")

      val topKStatement = s"SELECT * FROM(SELECT * FROM tv ORDER BY id LIMIT ${topKThreshold - 1})"
      assert(SparkDatasetHelper.optimizedPlanLimit(
        spark.sql(topKStatement).queryExecution) === Option(topKThreshold - 1))

      val collectLimitStatement =
        s"SELECT * FROM (SELECT * FROM tv ORDER BY id LIMIT $topKThreshold)"
      assert(SparkDatasetHelper.optimizedPlanLimit(
        spark.sql(collectLimitStatement).queryExecution) === Option(topKThreshold))
    }
  }

  test("isCommandExec") {
    var query = "set"
    assert(SparkDatasetHelper.isCommandExec(spark.sql(query)))
    query = "explain set"
    assert(SparkDatasetHelper.isCommandExec(spark.sql(query)))
    query = "show tables"
    assert(SparkDatasetHelper.isCommandExec(spark.sql(query)))
    query = "select * from VALUES(1),(2),(3),(4) AS t(id)"
    assert(!SparkDatasetHelper.isCommandExec(spark.sql(query)))
  }

  test("arrow compression config flows from the session conf into the produced IPC batches") {
    val codecKey = "spark.sql.execution.arrow.compression.codec"
    val levelKey = "spark.sql.execution.arrow.compression.zstd.level"
    // restore the shared engine session conf, otherwise the codec leaks into later suites/tests.
    // getOption/set/unset are all public RuntimeConfig APIs across Spark 3.3+/4.x.
    val restoreCodec = spark.conf.getOption(codecKey)
    val restoreLevel = spark.conf.getOption(levelKey)
    try {
      spark.conf.set(codecKey, "zstd")
      spark.conf.set(levelKey, "1")

      val plan = spark.range(0, 100).queryExecution.executedPlan
      val level1 = SparkDatasetHelper.toArrowBatchRdd(plan).collect().head
      spark.conf.set(levelKey, "19")
      val level19 = SparkDatasetHelper.toArrowBatchRdd(plan).collect().head

      // the configured zstd level must be honored, not silently dropped when the codec is
      // constructed
      assert(!level1.sameElements(level19))

      val allocator = ArrowUtils.rootAllocator.newChildAllocator("session-conf", 0, Long.MaxValue)
      try {
        val batch = deserialize(level19, allocator)
        try {
          val compression = batch.getBodyCompression
          assert(compression != null)
          assert(compression.getCodec == CompressionType.ZSTD)
          assert(batch.getLength == 100)

          // the compressed batch must be loadable through the same decompression factory the
          // client uses
          val arrowSchema = ArrowUtils.toArrowSchema(plan.schema, "UTC", true, false)
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          try {
            new VectorLoader(root, CommonsCompressionFactory.INSTANCE).load(batch)
            assert(root.getRowCount == 100)
          } finally {
            root.close()
          }
        } finally {
          batch.close()
        }
      } finally {
        allocator.close()
      }
    } finally {
      restoreCodec match {
        case Some(value) => spark.conf.set(codecKey, value)
        case None => spark.conf.unset(codecKey)
      }
      restoreLevel match {
        case Some(value) => spark.conf.set(levelKey, value)
        case None => spark.conf.unset(levelKey)
      }
    }
  }

  private def deserialize(bytes: Array[Byte], allocator: org.apache.arrow.memory.BufferAllocator)
      : ArrowRecordBatch = {
    MessageSerializer.deserializeRecordBatch(
      new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))),
      allocator)
  }
}
