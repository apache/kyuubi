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

package org.apache.kyuubi.spark.connector.tpcds

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.trino.tpcds.Table
import io.trino.tpcds.generator._
import org.apache.xbean.asm7.{ClassReader, ClassVisitor, MethodVisitor}
import org.apache.xbean.asm7.Opcodes.ASM7

object TPCDSTableUtils {

  // https://tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v3.2.0.pdf
  // Page 42 Table 3-2 Database Row Counts
  val tableAvrRowSizeInBytes: Map[String, Long] = Map(
    "call_center" -> 305,
    "catalog_page" -> 139,
    "catalog_returns" -> 166,
    "catalog_sales" -> 226,
    "customer" -> 132,
    "customer_address" -> 110,
    "customer_demographics" -> 42,
    "date_dim" -> 141,
    "household_demographics" -> 21,
    "income_band" -> 16,
    "inventory" -> 16,
    "item" -> 281,
    "promotion" -> 124,
    "reason" -> 38,
    "ship_mode" -> 56,
    "store" -> 263,
    "store_returns" -> 134,
    "store_sales" -> 164,
    "time_dim" -> 59,
    "warehouse" -> 117,
    "web_page" -> 96,
    "web_returns" -> 162,
    "web_sales" -> 226,
    "web_site" -> 292)

  def reviseColumnIndex[E <: Enum[E]](table: Table, index: Int): Int = {
    columnIndexRevisers(table.getName).getReviseColumnIndex(index)
  }

  private val columnIndexRevisers: Map[String, ColumnIndexReviser] = initColumnIndexRevisers()

  private def initColumnIndexRevisers[E <: Enum[E]](): Map[String, ColumnIndexReviser] = {
    Table.getBaseTables.asScala
      .filterNot(_.getName == "dbgen_version").map { table =>
        val first = table.getGeneratorColumns.head
        val classReader = getRowClassReader(table)
        val visitor = new GetValuesMethodVisitor(first.asInstanceOf[E])
        classReader.accept(visitor, 0)
        assert(visitor.getGeneratorColumn.length == table.getColumns.length)
        (table.getName, visitor)
      }.toMap
  }

  private def getRowClassReader(table: Table): ClassReader = {
    val rowClsName = "io.trino.tpcds.row." +
      table.getRowGeneratorClass.getSimpleName.replace("RowGenerator", "Row")
    val cls = Class.forName(rowClsName)
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    new ClassReader(resourceStream)
  }

  private trait ColumnIndexReviser {
    def getReviseColumnIndex(index: Int): Int
  }

  private class GetValuesMethodVisitor[T <: Enum[T]](firstColumn: T)
    extends ClassVisitor(ASM7) with ColumnIndexReviser {

    private val columns: ArrayBuffer[GeneratorColumn] = new ArrayBuffer[GeneratorColumn]()
    def getGeneratorColumn: Array[GeneratorColumn] = {
      firstColumn match {
        case CustomerGeneratorColumn.C_CUSTOMER_SK =>
          // io.trino.tpcds.row.CustomerRow.getValues, cLogin never gets set to anything
          columns.insert(15, CustomerGeneratorColumn.C_LOGIN)
        case _ =>
      }
      columns.toArray
    }

    override def getReviseColumnIndex(index: Int): Int = {
      getGeneratorColumn(index).getGlobalColumnNumber -
        firstColumn.asInstanceOf[GeneratorColumn].getGlobalColumnNumber
    }

    override def visitMethod(
        access: Int,
        name: String,
        descriptor: String,
        signature: String,
        exceptions: Array[String]): MethodVisitor = {
      if (name.equals("getValues")) {
        val columnType = firstColumn.getClass
        new MethodVisitor(ASM7) {
          override def visitFieldInsn(
              opcode: Int,
              owner: String,
              name: String,
              descriptor: String): Unit = {
            if (owner.replaceAll("/", ".").equals(columnType.getName)) {
              columns += Enum.valueOf(columnType.asInstanceOf[Class[T]], name)
                .asInstanceOf[GeneratorColumn]
            }
          }
        }
      } else {
        null
      }
    }
  }
}
