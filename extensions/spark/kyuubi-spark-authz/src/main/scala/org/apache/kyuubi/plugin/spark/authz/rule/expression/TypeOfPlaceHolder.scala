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

package org.apache.kyuubi.plugin.spark.authz.rule.expression

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}

import org.apache.kyuubi.plugin.spark.authz.util.WithInternalExpressionChild

case class TypeOfPlaceHolder(expr: Expression) extends UnaryExpression
  with WithInternalExpressionChild {
  override def dataType: DataType = StringType

  // Avoid fold constant expression by Spark Optimizer
  override def foldable: Boolean = false

  override def child: Expression = expr

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, _ => s"""UTF8String.fromString(${child.dataType.catalogString})""")
  }

  override def withNewChildInternal(newChild: Expression): Expression =
    copy(expr = newChild)
}
