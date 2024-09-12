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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class FileMergingRDD(
    @transient private val sparkSession: SparkSession,
    val dataSchema: StructType,
    val filePartitions: Array[MergingFilePartition])
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val onePartition = split.asInstanceOf[MergingFilePartition]
    new Iterator[InternalRow] with AutoCloseable {
      private[this] var currentRow: Option[InternalRow] = None

      override def hasNext: Boolean = currentRow.isEmpty

      override def next(): InternalRow = {
        currentRow = Some(onePartition.toInternalRow)
        currentRow.get
      }

      override def close(): Unit = {
        currentRow = None
      }
    }
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */

  override protected def getPartitions: Array[Partition] =
    filePartitions.map(_.asInstanceOf[Partition])

}
