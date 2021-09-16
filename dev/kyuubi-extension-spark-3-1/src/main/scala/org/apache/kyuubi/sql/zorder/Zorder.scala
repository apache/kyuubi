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

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{BinaryType, DataType}

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

case class Zorder(children: Seq[Expression]) extends ComplexTypeMergingExpression with Logging {
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
  private lazy val defaultNullValues: JMap[Int, Array[Byte]] = {
    val map = new JHashMap[Int, Array[Byte]](children.length)
    children.zipWithIndex
      .foreach {
        case (child, index) =>
          map.put(index, ZorderBytesUtils.toByte(
            ZorderBytesUtils.defaultValue(child.dataType)))
      }
    map
  }

  override def eval(input: InternalRow): Any = {
    val binaryArr = children.zipWithIndex.map {
      case (child: Expression, index) =>
        val v = child.eval(input)
        if (v == null) {
          defaultNullValues.get(index)
        } else {
          ZorderBytesUtils.toByte(v)
        }
    }
    ZorderBytesUtils.interleaveMultiByteArray(binaryArr.toArray)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val defaultValues = ctx.addReferenceObj("defaultValues", defaultNullValues)
    val binaryArray = ctx.freshName("binaryArray")
    val util = ZorderBytesUtils.getClass.getName.stripSuffix("$")
    val inputs = evals.zipWithIndex.map {
      case (eval, index) =>
        s"""
           |${eval.code}
           |if (${eval.isNull}) {
           |  $binaryArray[$index] = (byte[]) $defaultValues.get($index);
           |} else {
           |  $binaryArray[$index] = $util.toByte(${eval.value});
           |}
           |""".stripMargin
    }
    ev.copy(code =
      code"""
         |byte[] ${ev.value} = null;
         |byte[][] $binaryArray = new byte[${evals.length}][];
         |${inputs.mkString("\n")}
         |${ev.value} = $util.interleaveMultiByteArray($binaryArray);
         |""".stripMargin,
      isNull = FalseLiteral)
  }
}
