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

package org.apache.kyuubi.sql.compact

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

case class MergingFile(subGroupId: Int, name: String, length: Long) {
  override def toString: String =
    s"MergingFile(sub group id $subGroupId, name $name, length $length)"
}

object MergingFilePartition {
  private def toInternalRow(part: MergingFilePartition): InternalRow = {
    val projection = UnsafeProjection.create(CompactTable.smallFileCollectOutput)
    projection(InternalRow(
      part.groupId,
      UTF8String.fromString(part.location),
      UTF8String.fromString(part.dataSource),
      UTF8String.fromString(part.codec.orNull),
      ArrayData.toArrayData(part.smallFiles.map(f =>
        InternalRow(f.subGroupId, UTF8String.fromString(f.name), f.length)))))
  }
}

case class MergingFilePartition(
    groupId: Int,
    location: String,
    dataSource: String,
    codec: Option[String],
    smallFiles: Seq[MergingFile],
    index: Int = -1) extends Partition {
  override def toString: String = s"MergingFilePartition(index=$index,groupId=$groupId" +
    s"location $location,data source $dataSource,codec $codec," +
    s"small files ${smallFiles.mkString("[", ",", "]")})"

  def toInternalRow: InternalRow = MergingFilePartition.toInternalRow(this)
}
