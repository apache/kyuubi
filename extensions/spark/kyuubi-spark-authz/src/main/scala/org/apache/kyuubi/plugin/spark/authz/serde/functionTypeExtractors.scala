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

package org.apache.kyuubi.plugin.spark.authz.serde

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog

import org.apache.kyuubi.plugin.spark.authz.serde.FunctionExtractor.buildFunctionFromQualifiedName
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionType.{FunctionType, PERMANENT, SYSTEM, TEMP}
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionTypeExtractor.getFunctionType

object FunctionType extends Enumeration {
  type FunctionType = Value
  val PERMANENT, TEMP, SYSTEM = Value
}

trait FunctionTypeExtractor extends ((AnyRef, SparkSession) => FunctionType) with Extractor

object FunctionTypeExtractor {
  val functionTypeExtractors: Map[String, FunctionTypeExtractor] = {
    loadExtractorsToMap[FunctionTypeExtractor]
  }

  def getFunctionType(fi: FunctionIdentifier, catalog: SessionCatalog): FunctionType = {
    fi match {
      case temp if catalog.isTemporaryFunction(temp) =>
        TEMP
      case permanent if catalog.isPersistentFunction(permanent) =>
        PERMANENT
      case system if catalog.isRegisteredFunction(system) =>
        SYSTEM
      case _ =>
        TEMP
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ViewType
 */
class TempMarkerFunctionTypeExtractor extends FunctionTypeExtractor {
  override def apply(v1: AnyRef, spark: SparkSession): FunctionType = {
    if (v1.toString.toBoolean) {
      TEMP
    } else {
      PERMANENT
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.expressions.ExpressionInfo
 */
class ExpressionInfoFunctionTypeExtractor extends FunctionTypeExtractor {
  override def apply(v1: AnyRef, spark: SparkSession): FunctionType = {
    val function = lookupExtractor[ExpressionInfoFunctionExtractor].apply(v1)
    val fi = FunctionIdentifier(function.functionName, function.database)
    lookupExtractor[FunctionIdentifierFunctionTypeExtractor].apply(fi, spark)
  }
}

/**
 * org.apache.spark.sql.catalyst.FunctionIdentifier
 */
class FunctionIdentifierFunctionTypeExtractor extends FunctionTypeExtractor {
  override def apply(v1: AnyRef, spark: SparkSession): FunctionType = {
    val catalog = spark.sessionState.catalog
    val fi = v1.asInstanceOf[FunctionIdentifier]
    getFunctionType(fi, catalog)
  }
}

/**
 * String
 */
class FunctionNameFunctionTypeExtractor extends FunctionTypeExtractor {
  override def apply(v1: AnyRef, spark: SparkSession): FunctionType = {
    val catalog: SessionCatalog = spark.sessionState.catalog
    val qualifiedName: String = v1.asInstanceOf[String]
    val function = buildFunctionFromQualifiedName(qualifiedName)
    getFunctionType(FunctionIdentifier(function.functionName, function.database), catalog)
  }
}
