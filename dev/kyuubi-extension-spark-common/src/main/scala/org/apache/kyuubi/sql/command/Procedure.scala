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
import org.apache.spark.sql.KyuubiSparkSqlUtil._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BooleanType, StructType}

import org.apache.kyuubi.sql.command.KDPRegistry._

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

  override val outputType: StructType = kdpOutputStructType

  override def exec(args: InternalRow): Seq[Row] = {
    listProcedures().map { kdp =>
      Row.fromSeq(kdpOutput(kdp))
    }
  }
}

  case object DescribeKyuubiProcedure extends Procedure {
    override val parameters: Seq[ProcedureParameter] =
      Seq(ProcedureParameter.required("name", BooleanType))

    override val outputType: StructType = kdpOutputStructType

    override def exec(args: InternalRow): Seq[Row] = {
      val procedureName = args.getString(0)

      lookUpProcedure(procedureName).map { kdp =>
        Row.fromSeq(kdpOutput(kdp))
      }.toSeq

      lookUpProcedure(procedureName) match {
        case Some(kdp) => Seq(Row(kdpOutput(kdp)))

        case _ =>
          throwAnalysisException(
            s"no Kyuubi defined procedure found with name[$procedureName]")
          Seq.empty[Row]
      }
    }
}
