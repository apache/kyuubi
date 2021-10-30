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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object KDPRegistry {
  @transient
  var registeredProcedures = new ArrayBuffer[KyuubiDefinedProcedure]()

  val stop_engine: KyuubiDefinedProcedure = create(
    "stop_engine",
    StopEngineProcedure,
    "stop the spark engine",
    "1.4.0")

  val show_procedures: KyuubiDefinedProcedure = create(
    "show_procedures",
    ShowKyuubiProcedures,
    "show all the kyuubi defined procedures",
    "1.4.0")

  val desc_procedure: KyuubiDefinedProcedure = create(
    "desc_procedure",
    DescribeKyuubiProcedure,
    "describe a kyuubi defined procedures with name",
    "1.4.0",
    Seq("desc_proc"))

  def create(
    name: String,
    procedure: Procedure,
    description: String,
    since: String,
    alternativeNames: Seq[String] = Seq.empty[String]): KyuubiDefinedProcedure = {
    val kdp = KyuubiDefinedProcedure(name, procedure, description, since, alternativeNames)
    registeredProcedures += kdp
    kdp
  }

  def listProcedures(): Seq[KyuubiDefinedProcedure] = registeredProcedures

  def lookUpProcedure(name: String): Option[KyuubiDefinedProcedure] = {
    registeredProcedures.find { kdp =>
      kdp.name.equalsIgnoreCase(name) || kdp.alternativeNames.exists(_.equalsIgnoreCase(name))
    }
  }

  @transient
  lazy val kdpStructType = StructType(Array(
    StructField("name", StringType, false),
    StructField("alternative names", StringType, false),
    StructField("parameters", StringType, false),
    StructField("description", StringType, false),
    StructField("since", StringType, false)))

  def kdpDescription(kdp: KyuubiDefinedProcedure): Seq[String] = {
    Seq(kdp.name,
      kdp.alternativeNames.mkString(","),
      kdp.parameters.mkString("[", ",\n", "]"),
      kdp.description,
      kdp.since)
  }
}
