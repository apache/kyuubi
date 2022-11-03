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

package org.apache.kyuubi.parser.node

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.parser.node.translate.Translator
import org.apache.kyuubi.session.KyuubiSessionImpl

trait LogicalNode {

  def name(): String
}

/**
 * A node that can run on the kyuubi server
 */
trait RunnableNode extends LogicalNode {

  protected type R

  def shouldRunAsync: Boolean

  def execute(session: KyuubiSessionImpl): R

  def getNextRowSet: TRowSet

  def getResultSetSchema: TTableSchema = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName("Result")
    val desc = new TTypeDesc
    desc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
    tColumnDesc.setTypeDesc(desc)
    tColumnDesc.setPosition(0)
    val schema = new TTableSchema()
    schema.addToColumns(tColumnDesc)
    schema
  }
}

/**
 * Indicates the node to be translated
 */
trait TranslateNode extends LogicalNode {

  def accept(translator: Translator): String
}

/**
 * A pass through node, which is a flag indicating that the statement
 * will be sent directly to the engine without any operations.
 */
case class PassThroughNode() extends LogicalNode {

  override def name(): String = "Pass Through Node"
}
