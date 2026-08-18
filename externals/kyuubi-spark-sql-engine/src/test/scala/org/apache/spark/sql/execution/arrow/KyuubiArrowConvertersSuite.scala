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

package org.apache.spark.sql.execution.arrow

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import org.apache.arrow.compression.CommonsCompressionFactory
import org.apache.arrow.flatbuf.CompressionType
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{IntVector, VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.KyuubiFunSuite

/**
 * Test suite for the arrow compression support in [[KyuubiArrowConverters]]:
 *  - the configured zstd compression level must be honored when constructing the codec, i.e.
 *    it must not be silently dropped in favor of the default level
 *  - slice() must keep the decompression factory and the compression codec paired, so that
 *    compressed batches can be sliced when a LIMIT cuts across a batch boundary
 */
class KyuubiArrowConvertersSuite extends KyuubiFunSuite {

  private val timeZoneId = "UTC"

  private val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType)))

  // must match what ArrowUtils.toArrowSchema produces for the schema above, so that the
  // decompression side can load the batches
  private val arrowSchema = new Schema(List(
    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)).asJava)

  private def rows(count: Int): Iterator[InternalRow] =
    (0 until count).iterator.map(i => InternalRow(i, UTF8String.fromString(s"name_$i")))

  private def loadRoot(
      bytes: Array[Byte],
      allocator: BufferAllocator): (VectorSchemaRoot, ArrowRecordBatch) = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val recordBatch = MessageSerializer.deserializeRecordBatch(
      new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))),
      allocator)
    new VectorLoader(root, CommonsCompressionFactory.INSTANCE).load(recordBatch)
    (root, recordBatch)
  }

  private def assertRoundTrip(bytes: Array[Byte], expectedRows: Int): Unit = {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(s"round-trip-$expectedRows", 0, Long.MaxValue)
    try {
      val (root, recordBatch) = loadRoot(bytes, allocator)
      try {
        assert(root.getRowCount == expectedRows)
        val ids = root.getVector("id").asInstanceOf[IntVector]
        assert(ids.get(0) == 0)
        assert(ids.get(expectedRows - 1) == expectedRows - 1)
      } finally {
        root.close()
      }
      recordBatch.close()
    } finally {
      allocator.close()
    }
  }

  test("zstd compression level is honored and the batches round-trip") {
    val noneBytes = KyuubiArrowConverters
      .toBatchIterator(rows(100), schema, 1000, -1, -1, timeZoneId, null, 3)
      .toArray
      .head
    val level1 = KyuubiArrowConverters.slice(schema, timeZoneId, noneBytes, 0, 100, "zstd", 1)
    val level19 = KyuubiArrowConverters.slice(schema, timeZoneId, noneBytes, 0, 100, "zstd", 19)

    // the configured zstd level must be stored in the batch and therefore produce different
    // bytes, i.e. the configured level is not silently dropped when the codec is constructed
    assert(!level1.sameElements(level19))
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator("compression-level", 0, Long.MaxValue)
    try {
      val level1Batch = MessageSerializer.deserializeRecordBatch(
        new ReadChannel(Channels.newChannel(new ByteArrayInputStream(level1))),
        allocator)
      try {
        val bodyCompression = level1Batch.getBodyCompression
        assert(bodyCompression != null)
        assert(bodyCompression.getCodec == CompressionType.ZSTD)
      } finally {
        level1Batch.close()
      }
    } finally {
      allocator.close()
    }

    assertRoundTrip(level1, 100)
    assertRoundTrip(level19, 100)
  }

  test("slice cuts inside the head batch of compressed results") {
    val batches = KyuubiArrowConverters
      .toBatchIterator(rows(150), schema, 100, -1, -1, timeZoneId, "zstd", 3)
    val batch1 = batches.next() // 100 rows, compressed

    // the LIMIT cuts inside the head batch: 40 rows out of 100.
    // This is the only slice() call shape that SparkDatasetHelper.doCollectLimit performs in
    // production: it slices a batch only when the remaining row budget is smaller than the
    // batch size (size > rest); batches that fit entirely are passed through unsliced.
    val sliced = KyuubiArrowConverters.slice(schema, timeZoneId, batch1, 0, 40, "zstd", 3)
    assertRoundTrip(sliced, 40)

    // exhaust the remaining batch so that the iterator closes its allocator
    assert(batches.hasNext)
    batches.next()
    assert(!batches.hasNext)
  }

  test("reject unsupported lz4 compression codec") {
    val error = intercept[IllegalArgumentException] {
      KyuubiArrowConverters.toBatchIterator(rows(1), schema, 100, -1, -1, timeZoneId, "lz4", 3)
    }
    assert(error.getMessage.contains("Arrow compression codec lz4 is not supported by Kyuubi"))
  }
}
