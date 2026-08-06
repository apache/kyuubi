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

package org.apache.spark.sql.hive.kyuubi.connector

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, ExternalCatalogEvent}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{bucket, reference}
import org.apache.spark.sql.execution.datasources.orc.OrcFilters
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, StructType}

import org.apache.kyuubi.util.reflect.{DynClasses, DynConstructors, DynFields, DynMethods}

object HiveBridgeHelper {
  type HiveSessionCatalog = org.apache.spark.sql.hive.HiveSessionCatalog
  type HiveMetastoreCatalog = org.apache.spark.sql.hive.HiveMetastoreCatalog
  type HiveExternalCatalog = org.apache.spark.sql.hive.HiveExternalCatalog
  type NextIterator[U] = org.apache.spark.util.NextIterator[U]
  type HiveVersion = org.apache.spark.sql.hive.client.HiveVersion
  type InsertIntoHiveTable = org.apache.spark.sql.hive.execution.InsertIntoHiveTable

  val hive = org.apache.spark.sql.hive.client.hive
  val LogicalExpressions = org.apache.spark.sql.connector.expressions.LogicalExpressions
  val HiveClientImpl = org.apache.spark.sql.hive.client.HiveClientImpl
  val SparkHadoopWriterUtils = org.apache.spark.internal.io.SparkHadoopWriterUtils
  val CatalogV2Util = org.apache.spark.sql.connector.catalog.CatalogV2Util
  val HiveTableUtil = org.apache.spark.sql.hive.HiveTableUtil
  val HiveShim = org.apache.spark.sql.hive.HiveShim
  val InputFileBlockHolder = org.apache.spark.rdd.InputFileBlockHolder
  val HadoopTableReader = org.apache.spark.sql.hive.HadoopTableReader
  val SparkHadoopUtil = org.apache.spark.deploy.SparkHadoopUtil
  val Utils = org.apache.spark.util.Utils
  val CatalogV2Implicits = org.apache.spark.sql.connector.catalog.CatalogV2Implicits

  def postExternalCatalogEvent(sc: SparkContext, event: ExternalCatalogEvent): Unit = {
    sc.listenerBus.post(event)
  }

  implicit class TransformHelper(transforms: Seq[Transform]) {
    def convertTransforms: (Seq[String], Option[BucketSpec]) = {
      val identityCols = new mutable.ArrayBuffer[String]
      var bucketSpec = Option.empty[BucketSpec]

      transforms.map {
        case IdentityTransform(FieldReference(Seq(col))) =>
          identityCols += col

        case BucketTransform(numBuckets, col, sortCol) =>
          if (bucketSpec.nonEmpty) {
            throw new UnsupportedOperationException("Multiple bucket transforms are not supported.")
          }
          if (sortCol.isEmpty) {
            bucketSpec = Some(BucketSpec(numBuckets, col.map(_.fieldNames.mkString(".")), Nil))
          } else {
            bucketSpec = Some(BucketSpec(
              numBuckets,
              col.map(_.fieldNames.mkString(".")),
              sortCol.map(_.fieldNames.mkString("."))))
          }

        case transform =>
          throw new UnsupportedOperationException(
            s"Unsupported partition transform: $transform")
      }

      (identityCols.toSeq, bucketSpec)
    }
  }

  implicit class BucketSpecHelper(spec: BucketSpec) {
    def asTransform: Transform = {
      val references = spec.bucketColumnNames.map(col => reference(Seq(col)))
      if (spec.sortColumnNames.nonEmpty) {
        val sortedCol = spec.sortColumnNames.map(col => reference(Seq(col)))
        bucket(spec.numBuckets, references.toArray, sortedCol.toArray)
      } else {
        bucket(spec.numBuckets, references.toArray)
      }
    }
  }

  implicit class StructTypeHelper(structType: StructType) {
    def toAttributes: Seq[AttributeReference] = structType.map { field =>
      AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }
  }

  def toSQLValue(v: Any, t: DataType): String = Literal.create(v, t) match {
    case Literal(null, _) => "NULL"
    case Literal(v: Float, FloatType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else v.toString
    case l @ Literal(v: Double, DoubleType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else l.sql
    case l => l.sql
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quoteIfNeeded).mkString(".")
  }

  def orcConvertibleFilters(
      schema: StructType,
      caseSensitive: Boolean,
      dataFilters: Seq[Filter]): Seq[Filter] = {
    val dataTypeMap = OrcFilters.getSearchableTypeMap(schema, caseSensitive)
    OrcFilters.convertibleFilters(dataTypeMap, dataFilters)
  }

  def parquetConvertibleFilters(
      readDataSchema: StructType,
      dataFilters: Seq[Filter]): Seq[Filter] = {
    val sqlConf = SQLConf.get
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    // sqlConf.parquetFilterPushDownStringPredicate is added in 3.4+, so we use
    // spark.sql.parquet.filterPushdown.string.startsWith to remain compatible with Spark 3.3
    val pushDownStringPredicate =
      sqlConf.getConf(SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED)
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetSchema = new SparkToParquetSchemaConverter(sqlConf).convert(readDataSchema)
    val rebaseSpec = rebaseSpecCorrected
    val parquetFilters = new ParquetFilters(
      parquetSchema,
      pushDownDate,
      pushDownTimestamp,
      pushDownDecimal,
      pushDownStringPredicate,
      pushDownInFilterThreshold,
      isCaseSensitive,
      rebaseSpec)
    parquetFilters.convertibleFilters(dataFilters)
  }

  /**
   * `RebaseSpec(LegacyBehaviorPolicy.CORRECTED, None)` constructed via reflection so
   * the same code compiles against Spark 3.3 / 3.4 (where `LegacyBehaviorPolicy` is
   * nested in `SQLConf`) and Spark 3.5+ (SPARK-44538 promoted it to a top-level object
   * under `org.apache.spark.sql.internal`). Cached because the value is immutable.
   */
  private lazy val rebaseSpecCorrected: RebaseSpec = {
    val policyCls = DynClasses.builder()
      .impl("org.apache.spark.sql.internal.LegacyBehaviorPolicy$") // SPARK-44538: Spark 3.5+
      .impl("org.apache.spark.sql.internal.SQLConf$LegacyBehaviorPolicy$") // Spark 3.3 / 3.4
      .buildChecked()

    val policyModule = DynFields.builder()
      .impl(policyCls, "MODULE$")
      .buildStaticChecked[AnyRef]()
      .get()

    val correctedPolicy = DynMethods.builder("CORRECTED")
      .impl(policyCls)
      .buildChecked()
      .invoke[AnyRef](policyModule)

    // `RebaseSpec.mode` is typed as `LegacyBehaviorPolicy.Value`, which after
    // erasure becomes `scala.Enumeration$Value` in the constructor signature.
    // Use the erased type explicitly so `DynConstructors` can match exactly.
    val enumValueCls = Class.forName("scala.Enumeration$Value")
    DynConstructors.builder()
      .impl(
        "org.apache.spark.sql.catalyst.util.RebaseDateTime$RebaseSpec",
        enumValueCls,
        classOf[Option[_]])
      .buildChecked[RebaseSpec]()
      .newInstance(correctedPolicy, None)
  }
}
