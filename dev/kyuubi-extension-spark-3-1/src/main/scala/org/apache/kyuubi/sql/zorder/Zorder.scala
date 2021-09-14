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

package org.apache.kyuubi.sql.zorder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, DataType}

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

case class Zorder(children: Seq[Expression]) extends Expression with CodegenFallback {
  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = false
  override def dataType: DataType = BinaryType
  override def prettyName: String = "zorder"

  override def checkInputDataTypes(): TypeCheckResult = {
    try {
      defaultNullValues
      TypeCheckResult.TypeCheckSuccess
    } catch {
      case e: KyuubiSQLExtensionException =>
        TypeCheckResult.TypeCheckFailure(e.getMessage)
    }
  }

  @transient
  private lazy val defaultNullValues: Seq[Any] = {
    children.map(child => ZorderBytesUtils.defaultValue(child.dataType))
  }

  override def eval(input: InternalRow): Any = {
    val evaluated = children.zipWithIndex.map { case (child: Expression, index) =>
      val v = child.eval(input)
      if (v == null) {
        defaultNullValues(index)
      } else {
        v
      }
    }

    val binaryArr = evaluated.map(ZorderBytesUtils.toByte).toArray
    ZorderBytesUtils.interleaveMultiByteArray(binaryArr)
  }
}
