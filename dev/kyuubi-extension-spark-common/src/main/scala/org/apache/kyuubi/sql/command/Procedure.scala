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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

trait Procedure {
  val parameters: Seq[ProcedureParameter]
  val outputType: StructType = StructType(Nil)
  def exec(args: InternalRow): Seq[Row]
}

case object StopEngineProcedure extends Procedure {
  override val parameters: Seq[ProcedureParameter] = Seq.empty[ProcedureParameter]

  override def exec(args: InternalRow): Seq[Row] = {
    SparkSession.getActiveSession.foreach(_.stop())
    Seq.empty[Row]
  }
}

case object ShowKyuubiProcedures extends Procedure {
  override val parameters: Seq[ProcedureParameter] = Seq.empty[ProcedureParameter]

  override val outputType: StructType = StructType(Array(
    StructField("name", StringType, false),
    StructField("parameters", StringType, false),
    StructField("description", StringType, false),
    StructField("since", StringType, false)))

  override def exec(args: InternalRow): Seq[Row] = {
    KDPRegistry.listProcedures().map { kdp =>
      Row(kdp.name, kdp.parameters.mkString("[", ",", "]"), kdp.description, kdp.since)
    }
  }
}

  case object DescribeKyuubiProcedure extends Procedure {
    override val parameters: Seq[ProcedureParameter] =
      Seq(ProcedureParameter.required("name", BooleanType))

    override val outputType: StructType = StructType(Array(
      StructField("name", StringType, false),
      StructField("parameters", StringType, false),
      StructField("description", StringType, false),
      StructField("since", StringType, false)))

    override def exec(args: InternalRow): Seq[Row] = {
      val procedureName = args.getString(0)
      KDPRegistry.lookUpProcedure(procedureName).map { kdp =>
        Row(kdp.name, kdp.parameters.mkString("[", ",", "]"), kdp.description, kdp.since)
      }.toSeq
    }
}
