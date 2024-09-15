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

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters.createToScalaConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class SmallFileListExec(child: SparkPlan) extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val structType = DataTypeUtils.fromAttributes(output)
    child.execute().mapPartitionsWithIndex { (partIndex, iterator) =>
      val converter = createToScalaConverter(structType)

      iterator.map(converter).map {
        case Row(
              groupId: Int,
              location: String,
              dataSource: String,
              codec,
              smallFileNameAndLength: mutable.WrappedArray[_]) =>
          val codecOption = Option(codec).map(_.toString)

          MergingFilePartition(
            groupId,
            location,
            dataSource,
            codecOption,
            smallFileNameAndLength.map {
              case Row(subGroupId: Int, name: String, length: Long) =>
                MergingFile(subGroupId, name, length)
            }.toList)
            .toInternalRow
        case unknown =>
          throw new IllegalArgumentException(s"Unexpected row: $unknown, class ${unknown.getClass}")
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    SmallFileListExec(newChild)
  }
}
