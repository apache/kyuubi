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

package org.apache.kyuubi.plugin.spark.authz.gen

import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionType._

object Scans extends CommandSpecs[ScanSpec] {

  val HiveTableRelation = {
    val r = "org.apache.spark.sql.catalyst.catalog.HiveTableRelation"
    val tableDesc =
      ScanDesc(
        "tableMeta",
        classOf[CatalogTableTableExtractor])
    ScanSpec(r, Seq(tableDesc))
  }

  val LogicalRelation = {
    val r = "org.apache.spark.sql.execution.datasources.LogicalRelation"
    val tableDesc =
      ScanDesc(
        "catalogTable",
        classOf[CatalogTableOptionTableExtractor])
    val uriDesc = UriDesc("relation", classOf[BaseRelationFileIndexURIExtractor])
    ScanSpec(r, Seq(tableDesc), uriDescs = Seq(uriDesc))
  }

  val DataSourceV2Relation = {
    val r = "org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation"
    val tableDesc =
      ScanDesc(
        null,
        classOf[DataSourceV2RelationTableExtractor])
    ScanSpec(r, Seq(tableDesc))
  }

  val PermanentViewMarker = {
    val r = "org.apache.kyuubi.plugin.spark.authz.rule.permanentview.PermanentViewMarker"
    val tableDesc =
      ScanDesc(
        "catalogTable",
        classOf[CatalogTableTableExtractor])
    ScanSpec(r, Seq(tableDesc))
  }

  val HiveSimpleUDF = {
    ScanSpec(
      "org.apache.spark.sql.hive.HiveSimpleUDF",
      Seq.empty,
      Seq(FunctionDesc(
        "name",
        classOf[QualifiedNameStringFunctionExtractor],
        functionTypeDesc = Some(FunctionTypeDesc(
          "name",
          classOf[FunctionNameFunctionTypeExtractor],
          Seq(TEMP, SYSTEM))),
        isInput = true)))
  }

  val HiveGenericUDF = HiveSimpleUDF.copy(classname = "org.apache.spark.sql.hive.HiveGenericUDF")

  val HiveUDAFFunction = HiveSimpleUDF.copy(classname =
    "org.apache.spark.sql.hive.HiveUDAFFunction")

  val HiveGenericUDTF = HiveSimpleUDF.copy(classname = "org.apache.spark.sql.hive.HiveGenericUDTF")

  override def specs: Seq[ScanSpec] = Seq(
    HiveTableRelation,
    LogicalRelation,
    DataSourceV2Relation,
    PermanentViewMarker,
    HiveSimpleUDF,
    HiveGenericUDF,
    HiveUDAFFunction,
    HiveGenericUDTF)
}
