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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{BinaryType, DataType}

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

abstract class ZorderBase extends Expression {
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
  private[this] lazy val defaultNullValues: Array[Any] =
    children.map(_.dataType)
      .map(ZorderBytesUtils.defaultValue)
      .toArray

  override def eval(input: InternalRow): Any = {
    val childrenValues = children.zipWithIndex.map {
      case (child: Expression, index) =>
        val v = child.eval(input)
        if (v == null) {
          defaultNullValues(index)
        } else {
          v
        }
    }
    ZorderBytesUtils.interleaveBits(childrenValues.toArray)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val defaultValues = ctx.addReferenceObj("defaultValues", defaultNullValues)
    val values = ctx.freshName("values")
    val util = ZorderBytesUtils.getClass.getName.stripSuffix("$")
    val inputs = evals.zipWithIndex.map {
      case (eval, index) =>
        s"""
           |${eval.code}
           |if (${eval.isNull}) {
           |  $values[$index] = $defaultValues[$index];
           |} else {
           |  $values[$index] = ${eval.value};
           |}
           |""".stripMargin
    }
    ev.copy(code =
      code"""
         |byte[] ${ev.value} = null;
         |Object[] $values = new Object[${evals.length}];
         |${inputs.mkString("\n")}
         |${ev.value} = $util.interleaveBits($values);
         |""".stripMargin,
      isNull = FalseLiteral)
  }
}

case class Zorder(children: Seq[Expression]) extends ZorderBase {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
