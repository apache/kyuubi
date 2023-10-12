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

import java.util.Properties

import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{HadoopTableReader, HiveShim}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class HivePartitionedReader(
    file: PartitionedFile,
    reader: PartitionReader[Writable],
    tableDesc: TableDesc,
    broadcastHiveConf: Broadcast[SerializableConfiguration],
    nonPartitionReadDataKeys: Seq[Attribute],
    bindPartitionOpt: Option[HivePartition],
    charset: String = "utf-8") extends PartitionReader[InternalRow] with Logging {

  private val hiveConf = broadcastHiveConf.value.value

  private val tableDeser = tableDesc.getDeserializerClass.newInstance()
  tableDeser.initialize(hiveConf, tableDesc.getProperties)

  private val localDeser: Deserializer = bindPartitionOpt match {
    case Some(bindPartition) if bindPartition.getDeserializer != null =>
      val tableProperties = tableDesc.getProperties
      val props = new Properties(tableProperties)
      val deserializer =
        bindPartition.getDeserializer.getClass.asInstanceOf[Class[Deserializer]].newInstance()
      deserializer.initialize(hiveConf, props)
      deserializer
    case _ => tableDeser
  }

  private val internalRow = new SpecificInternalRow(nonPartitionReadDataKeys.map(_.dataType))

  private val soi: StructObjectInspector =
    if (localDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
      localDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        localDeser.getObjectInspector,
        tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
    }

  private val (fieldRefs, fieldOrdinals) = nonPartitionReadDataKeys.zipWithIndex
    .map { case (attr, ordinal) =>
      soi.getStructFieldRef(attr.name) -> ordinal
    }.toArray.unzip

  private val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
    _.getFieldObjectInspector match {
      case oi: BooleanObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
      case oi: ByteObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
      case oi: ShortObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
      case oi: IntObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
      case oi: LongObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
      case oi: FloatObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
      case oi: DoubleObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
      case oi: HiveVarcharObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) =>
          row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
      case oi: HiveCharObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) =>
          row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
      case oi: HiveDecimalObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) =>
          row.update(ordinal, HiveShim.toCatalystDecimal(oi, value))
      case oi: TimestampObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) =>
          row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(oi.getPrimitiveJavaObject(value)))
      case oi: DateObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) =>
          row.setInt(ordinal, DateTimeUtils.fromJavaDate(oi.getPrimitiveJavaObject(value)))
      case oi: BinaryObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) =>
          row.update(ordinal, oi.getPrimitiveJavaObject(value))
      case oi =>
        logDebug("HiveInspector class: " + oi.getClass.getName + ", charset: " + charset)
        val unwrapper = HadoopTableReader.unwrapperFor(oi)
        (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
    }
  }

  private val converter =
    ObjectInspectorConverters.getConverter(localDeser.getObjectInspector, soi)

  private def fillObject(value: Writable, mutableRow: InternalRow): InternalRow = {
    val raw = converter.convert(localDeser.deserialize(value))
    var i = 0
    val length = fieldRefs.length
    while (i < length) {
      val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
      if (fieldValue == null) {
        mutableRow.setNullAt(fieldOrdinals(i))
      } else {
        unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
      }
      i += 1
    }
    mutableRow
  }

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = fillObject(reader.get(), internalRow)

  override def close(): Unit = reader.close()

  override def toString: String = file.toString
}
