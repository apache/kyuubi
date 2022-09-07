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

package org.apache.kyuubi.spark.connector.hive.read

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hive.ql.metadata.{Table => TableHive}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{hiveShim, hiveTableUtil}
import org.apache.spark.sql.types.StructType

object HiveReader {

  def initializeHiveConf(
      hiveTable: TableHive,
      hiveConf: Configuration,
      dataSchema: StructType,
      readDataSchema: StructType): Unit = {
    // Build tableDesc from hiveTable
    val tableDesc = getTableDec(hiveTable)
    // Set required columns and serde columns to hiveConf according to schema
    addColumnMetadataToConf(tableDesc, hiveConf, dataSchema, readDataSchema)
    // Copy hive table properties to hiveConf. For example,
    // initial job conf to read files with specified format
    hiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, hiveConf, false)
  }

  private def addColumnMetadataToConf(
      tableDesc: TableDesc,
      hiveConf: Configuration,
      dataSchema: StructType,
      readDataSchema: StructType): Unit = {
    // Set required column id and name to hiveConf,
    // to specified read columns without partition schema of Hive file reader
    val neededColumnNames = readDataSchema.map(_.name)
    val neededColumnIDs =
      readDataSchema.map(field => Integer.valueOf(dataSchema.fields.indexOf(field)))

    hiveShim.appendReadColumns(hiveConf, neededColumnIDs, neededColumnNames)

    val deserializer = tableDesc.getDeserializerClass.newInstance
    deserializer.initialize(hiveConf, tableDesc.getProperties)

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs.asScala
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, dataSchema.map(_.name).mkString(","))
  }

  def getTableDec(hiveTable: TableHive): TableDesc = {
    new TableDesc(
      hiveTable.getInputFormatClass,
      hiveTable.getOutputFormatClass,
      hiveTable.getMetadata)
  }

  def toAttributes(structType: StructType): Seq[AttributeReference] =
    structType.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  def getInputFormat(
      ifc: Class[_ <: InputFormat[Writable, Writable]],
      conf: JobConf): InputFormat[Writable, Writable] = {
    val newInputFormat = ReflectionUtils.newInstance(ifc.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[Writable, Writable]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }
}
