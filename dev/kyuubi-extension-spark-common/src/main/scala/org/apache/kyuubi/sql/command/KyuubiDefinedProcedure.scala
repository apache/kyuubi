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

package org.apache.kyuubi.sql.command

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * A wrapper for [[Procedure]]
 *
 * @param name procedure name
 * @param procedure kyuubi procedure
 * @param description procedure description
 */
case class KyuubiDefinedProcedure(
    name: String,
    procedure: Procedure,
    description: String,
    since: String,
    alternativeNames: Seq[String] = Seq.empty[String]) {
  def outputType: StructType = procedure.outputType
  def parameters: Seq[ProcedureParameter] = procedure.parameters
  def exec(args: InternalRow): Seq[Row] = procedure.exec(args)
}
