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

import org.apache.spark.sql.catalyst.analysis.UnresolvedUnaryNode
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafParsedStatement, LogicalPlan}
import org.apache.spark.sql.types._

object CompactTable {
  private val fileLocAndSizeStructArrayType: ArrayType =
    DataTypes.createArrayType(DataTypes.createStructType(Array(
      DataTypes.createStructField("sub_group_id", IntegerType, false),
      DataTypes.createStructField("name", StringType, false),
      DataTypes.createStructField("length", LongType, false))))

  val smallFileCollectOutput: StructType = DataTypes.createStructType(Array(
    DataTypes.createStructField("group_id", IntegerType, false),
    DataTypes.createStructField("location", StringType, false),
    DataTypes.createStructField("data_source", StringType, false),
    DataTypes.createStructField("codec", StringType, true),
    DataTypes.createStructField("smallFiles", fileLocAndSizeStructArrayType, false)))

  val smallFileCollectOutputAttribute: Seq[AttributeReference] = smallFileCollectOutput
    .map(field => AttributeReference(field.name, field.dataType, field.nullable)())

  val mergedFilesCachedTableName = "v_merged_files"
  val mergeMetadataKey = "spark.sql.compact.parquet.metadata.merge"
}

trait CompactTableOption

object CompactTableOptions {
  def apply(options: Option[String]): CompactTableOption = options.map(_.toLowerCase) match {
    case Some("retain") => RetainStagingFolder
    case Some("list") => DryRun
    case _ => CleanupStagingFolder
  }

  case object CleanupStagingFolder extends CompactTableOption

  case object RetainStagingFolder extends CompactTableOption

  case object DryRun extends CompactTableOption
}

case class CompactTable(
    child: LogicalPlan,
    targetSizeInBytes: Option[Long],
    options: CompactTableOption) extends UnresolvedUnaryNode {
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    CompactTable(newChild, targetSizeInBytes, options)
  }
}

case class CompactTableStatement(
    tableParts: Seq[String],
    targetSizeInMB: Option[Long],
    options: CompactTableOption) extends LeafParsedStatement

case class RecoverCompactTableStatement(tableParts: Seq[String])
  extends LeafParsedStatement

case class RecoverCompactTable(child: LogicalPlan) extends UnresolvedUnaryNode {
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    RecoverCompactTable(newChild)
  }
}
