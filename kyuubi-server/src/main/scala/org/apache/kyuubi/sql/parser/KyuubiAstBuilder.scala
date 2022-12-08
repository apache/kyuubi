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

package org.apache.kyuubi.sql.parser

import org.apache.kyuubi.sql.{KyuubiSqlBaseParser, KyuubiSqlBaseParserBaseVisitor}
import org.apache.kyuubi.sql.KyuubiSqlBaseParser.SingleStatementContext
import org.apache.kyuubi.sql.plan.{KyuubiTreeNode, PassThroughNode}
import org.apache.kyuubi.sql.plan.command.{DescribeSession, RunnableCommand}

class KyuubiAstBuilder extends KyuubiSqlBaseParserBaseVisitor[AnyRef] {

  override def visitSingleStatement(ctx: SingleStatementContext): KyuubiTreeNode = {
    visit(ctx.statement).asInstanceOf[KyuubiTreeNode]
  }

  override def visitPassThrough(ctx: KyuubiSqlBaseParser.PassThroughContext): KyuubiTreeNode = {
    PassThroughNode()
  }

  override def visitRunnable(ctx: KyuubiSqlBaseParser.RunnableContext): RunnableCommand = {
    val command = visit(ctx.runnableCommand()).asInstanceOf[RunnableCommand]
    if (ctx.KYUUBIADMIN() != null) {
      command.setRole(isAdmin = true)
    }
    command
  }

  override def visitDescribeSession(ctx: KyuubiSqlBaseParser.DescribeSessionContext)
      : RunnableCommand = {
    DescribeSession()
  }
}
