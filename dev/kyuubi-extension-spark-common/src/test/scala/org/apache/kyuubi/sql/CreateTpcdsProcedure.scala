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
package org.apache.kyuubi.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import org.apache.kyuubi.sql.call.procedure.{Procedure, ProcedureParameter}

class CreateTpcdsProcedure extends Procedure {

  override def parameters(): Array[ProcedureParameter] = Array(
    ProcedureParameter.required("sf", DataTypes.IntegerType),
    ProcedureParameter.required("db", DataTypes.StringType),
    ProcedureParameter.required("format", DataTypes.StringType),
    ProcedureParameter.optional("parallel", DataTypes.IntegerType))

  override def outputType(): StructType = new StructType(Array[StructField](
    new StructField("result", DataTypes.LongType, false, Metadata.empty)));

  override def call(args: InternalRow): Array[InternalRow] = {
    val config = new java.util.HashMap[String, String]()
    val params = parameters()
    config.put(params(0).name(), String.valueOf(args.getInt(0)))
    config.put(params(1).name(), args.getString(1))
    config.put(params(2).name(), args.getString(2))
    config.put(params(3).name(), String.valueOf(args.getInt(3)))
    Array.empty
  }

  override def getIdentifier: Identifier = Identifier.of(Array("spark", "kyuubi"), "create_tpcds")
}
