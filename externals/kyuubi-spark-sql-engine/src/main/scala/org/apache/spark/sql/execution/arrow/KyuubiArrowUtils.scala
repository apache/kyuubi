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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

object KyuubiArrowUtils {
  val rootAllocator = new RootAllocator(Long.MaxValue)
    .newChildAllocator("ReadIntTest", 0, Long.MaxValue)
  //    BufferAllocator allocator =
  //        ArrowUtils.rootAllocator.newChildAllocator("ReadIntTest", 0, Long.MAX_VALUE);
  def slice(bytes: Array[Byte], start: Int, length: Int): Array[Byte] = {
    val in = new ByteArrayInputStream(bytes)
    val out = new ByteArrayOutputStream()

    var reader: ArrowStreamReader = null
    try {
      reader = new ArrowStreamReader(in, rootAllocator)
//      reader.getVectorSchemaRoot.getSchema
      reader.loadNextBatch()
      val root = reader.getVectorSchemaRoot.slice(start, length)
//      val loader = new VectorLoader(root)
      val writer = new ArrowStreamWriter(root, null, out)
      writer.start()
      writer.writeBatch()
      writer.end()
      writer.close()
      out.toByteArray
    } finally {
      if (reader != null) {
        reader.close()
      }
      in.close()
      out.close()
    }
  }

  def sliceV2(
      schema: StructType,
      timeZoneId: String,
      bytes: Array[Byte],
      start: Int,
      length: Int): Array[Byte] = {
    val in = new ByteArrayInputStream(bytes)
    val out = new ByteArrayOutputStream()

    try {
//      reader = new ArrowStreamReader(in, rootAllocator)
//      //      reader.getVectorSchemaRoot.getSchema
//      reader.loadNextBatch()
//      println("bytes......" + bytes.length)
//      println("rowCount......" + reader.getVectorSchemaRoot.getRowCount)
//      val root = reader.getVectorSchemaRoot.slice(start, length)

      val recordBatch = MessageSerializer.deserializeRecordBatch(
        new ReadChannel(Channels.newChannel(in)),
        rootAllocator)
      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)

      val root = VectorSchemaRoot.create(arrowSchema, rootAllocator)
      val vectorLoader = new VectorLoader(root)
      vectorLoader.load(recordBatch)
      recordBatch.close()

      val unloader = new VectorUnloader(root.slice(start, length))
      val writeChannel = new WriteChannel(Channels.newChannel(out))
      val batch = unloader.getRecordBatch()
      MessageSerializer.serialize(writeChannel, batch)
      batch.close()
      out.toByteArray()
    } finally {
      in.close()
      out.close()
    }
  }
}
