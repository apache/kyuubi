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

package org.apache.kyuubi.plugin.lineage.events

import org.apache.spark.scheduler.SparkListenerEvent

case class ColumnLineage(column: String, originalColumns: Set[String])

/**
 * @param inputTables the tables of the operation will read
 * @param outputTables the tables of the operation will write
 * @param columnLineage the output columns are associated to columns of the real table's columns
 */

class Lineage(
    val inputTables: List[String],
    val outputTables: List[String],
    val columnLineage: List[ColumnLineage]) {

  override def equals(other: Any): Boolean = other match {
    case otherLineage: Lineage =>
      otherLineage.inputTables == inputTables && otherLineage.outputTables == outputTables &&
      otherLineage.columnLineage == columnLineage
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def toString: String = {
    s"inputTables($inputTables)\n" +
      s"outputTables($outputTables)\n" +
      s"columnLineage($columnLineage)"
  }
}

object Lineage {
  def apply(
      inputTables: List[String],
      outputTables: List[String],
      columnLineage: List[(String, Set[String])]): Lineage = {
    val newColumnLineage = columnLineage.map {
      case (column, originalColumns) =>
        ColumnLineage(column, originalColumns)
    }
    new Lineage(inputTables, outputTables, newColumnLineage)
  }
}

case class OperationLineageEvent(
    executionId: Long,
    eventTime: Long,
    lineage: Option[Lineage],
    exception: Option[Throwable]) extends SparkListenerEvent
