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

package org.apache.kyuubi.tpcds

import java.util.{List => JList}

import com.google.common.collect.AbstractIterator
import io.trino.tpcds.{Parallel, Session, Table}
import io.trino.tpcds.row.generator.AbstractRowGenerator

private[tpcds] object TPCDSRowIterator {
  def apply(table: Table, session: Session): TPCDSRowIterator = {
    val boundaries = Parallel.splitWork(table, session)
    new TPCDSRowIterator(table, session, boundaries.getFirstRow, boundaries.getLastRow)
  }
}

private[tpcds] class TPCDSRowIterator(
    table: Table,
    session: Session,
    startingRowNumber: Long,
    endingRowNumber: Long) extends AbstractIterator[JList[String]] {

  require(startingRowNumber >= 1, s"Invalid starting row: $startingRowNumber")
  require(
    endingRowNumber <= session.getScaling.getRowCount(table),
    s"Ending row exceeds the number of rows in $table: $endingRowNumber")

  private def newGenerator(sourceTable: Table): AbstractRowGenerator =
    sourceTable.getRowGeneratorClass.asSubclass(classOf[AbstractRowGenerator])
      .getDeclaredConstructor().newInstance()

  private val rowGenerator = newGenerator(table)
  private val parentRowGenerator = if (table.isChild) Some(newGenerator(table.getParent)) else None
  private val childRowGenerator = if (table.hasChild) Some(newGenerator(table.getChild)) else None
  private val generators = Seq(table -> rowGenerator) ++
    parentRowGenerator.map(table.getParent -> _) ++ childRowGenerator.map(table.getChild -> _)
  private var rowNumber = startingRowNumber

  // tpcds 1.4 narrows the starting row to Int. Seek with Long offsets so partitions
  // beyond Int.MaxValue use the same random streams as sequential generation.
  generators.foreach { case (sourceTable, generator) =>
    sourceTable.getGeneratorColumns.foreach { column =>
      generator.getRandomNumberStream(column).skipRows(startingRowNumber - 1)
    }
  }

  override protected def computeNext(): JList[String] = {
    while (rowNumber <= endingRowNumber) {
      val result = rowGenerator.generateRowAndChildRows(
        rowNumber,
        session,
        parentRowGenerator.orNull,
        childRowGenerator.orNull)
      if (result.shouldEndRow) {
        generators.foreach { case (_, generator) => generator.consumeRemainingSeedsForRow() }
        rowNumber += 1
      }
      if (!result.getRowAndChildRows.isEmpty) {
        // Only the first row belongs to the requested table; subsequent rows are its children.
        return result.getRowAndChildRows.get(0).getValues
      }
    }
    endOfData()
  }
}
