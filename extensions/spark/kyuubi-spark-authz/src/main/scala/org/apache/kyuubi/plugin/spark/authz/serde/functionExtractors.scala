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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

import org.apache.kyuubi.plugin.spark.authz.serde.FunctionExtractor.buildFunctionFromQualifiedName

trait FunctionExtractor extends (AnyRef => Function) with Extractor

object FunctionExtractor {
  val functionExtractors: Map[String, FunctionExtractor] = {
    loadExtractorsToMap[FunctionExtractor]
  }

  private[authz] def buildFunctionFromQualifiedName(qualifiedName: String): Function = {
    val parts: Array[String] = qualifiedName.split("\\.")
    val (catalog, database, functionName) = if (parts.length == 3) {
      (Some(parts.head), Some(parts.tail.head), parts.last)
    } else if (parts.length == 2) {
      (None, Some(parts.head), parts.last)
    } else {
      (None, None, qualifiedName)
    }
    Function(catalog, database, functionName)
  }
}

/**
 * String
 */
class StringFunctionExtractor extends FunctionExtractor {
  override def apply(v1: AnyRef): Function = {
    Function(None, None, v1.asInstanceOf[String])
  }
}

/**
 *  * String
 */
class QualifiedNameStringFunctionExtractor extends FunctionExtractor {
  override def apply(v1: AnyRef): Function = {
    val qualifiedName: String = v1.asInstanceOf[String]
    buildFunctionFromQualifiedName(qualifiedName)
  }
}

/**
 * org.apache.spark.sql.catalyst.FunctionIdentifier
 */
class FunctionIdentifierFunctionExtractor extends FunctionExtractor {
  override def apply(v1: AnyRef): Function = {
    val identifier = v1.asInstanceOf[FunctionIdentifier]
    Function(None, identifier.database, identifier.funcName)
  }
}

/**
 * org.apache.spark.sql.catalyst.expressions.ExpressionInfo
 */
class ExpressionInfoFunctionExtractor extends FunctionExtractor {
  override def apply(v1: AnyRef): Function = {
    val info = v1.asInstanceOf[ExpressionInfo]
    Function(None, Option(info.getDb), info.getName)
  }
}
