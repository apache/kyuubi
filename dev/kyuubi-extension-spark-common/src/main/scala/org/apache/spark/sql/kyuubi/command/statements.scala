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

package org.apache.spark.sql.kyuubi.command

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.ParsedStatement

case class ExecStatement(name: String, args: Seq[ExecArgument]) extends ParsedStatement

/**
 * An argument in a EXEC statement.
 */
sealed trait ExecArgument {
  def expr: Expression
}

/**
 * An argument in a CALL statement identified by name.
 */
case class NamedArgument(name: String, expr: Expression) extends ExecArgument

/**
 * An argument in a CALL statement identified by position.
 */
case class PositionalArgument(expr: Expression) extends ExecArgument
