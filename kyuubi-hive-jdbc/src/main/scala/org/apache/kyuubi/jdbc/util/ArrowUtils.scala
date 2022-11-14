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

package org.apache.kyuubi.jdbc.util

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.hive.service.rpc.thrift.TTypeId

import org.apache.kyuubi.jdbc.hive.JdbcColumnAttributes

object ArrowUtils {

  val rootAllocator = new RootAllocator(Long.MaxValue)

  def toArrowSchemaJava(
      columnNames: java.util.List[String],
      ttypes: java.util.List[TTypeId],
      columnAttributes: java.util.List[JdbcColumnAttributes],
      timeZoneId: String): Schema = {
    toArrowSchema(columnNames.asScala, ttypes.asScala, columnAttributes.asScala, timeZoneId)
  }
  //  /** Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType */
  //  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
  //    new Schema(schema.map { field =>
  //      toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
  //    }.asJava)
  //  }

  def toArrowSchema(
      columnNames: Seq[String],
      ttypes: Seq[TTypeId],
      columnAttributes: Seq[JdbcColumnAttributes],
      timeZoneId: String): Schema = {
    new Schema(
      (columnNames, ttypes, columnAttributes).zipped.map {
        case (name, tpe, arrtribute) => toArrowField(name, tpe, Option(arrtribute), timeZoneId)
      }.asJava)
  }
//  def toTTypeId(typ: DataType): TTypeId = typ match {
//    case NullType => TTypeId.NULL_TYPE
//    case BooleanType => TTypeId.BOOLEAN_TYPE
//    case ByteType => TTypeId.TINYINT_TYPE
//    case ShortType => TTypeId.SMALLINT_TYPE
//    case IntegerType => TTypeId.INT_TYPE
//    case LongType => TTypeId.BIGINT_TYPE
//    case FloatType => TTypeId.FLOAT_TYPE
//    case DoubleType => TTypeId.DOUBLE_TYPE
//    case StringType => TTypeId.STRING_TYPE
//    case _: DecimalType => TTypeId.DECIMAL_TYPE
//    case DateType => TTypeId.DATE_TYPE
//    // TODO: Shall use TIMESTAMPLOCALTZ_TYPE, keep AS-IS now for
//    // unnecessary behavior change
//    case TimestampType => TTypeId.TIMESTAMP_TYPE
//    case TimestampNTZType => TTypeId.TIMESTAMP_TYPE
//    case BinaryType => TTypeId.BINARY_TYPE
//    case CalendarIntervalType => TTypeId.STRING_TYPE
//    case _: DayTimeIntervalType => TTypeId.INTERVAL_DAY_TIME_TYPE
//    case _: YearMonthIntervalType => TTypeId.INTERVAL_YEAR_MONTH_TYPE
//    case _: ArrayType => TTypeId.ARRAY_TYPE
//    case _: MapType => TTypeId.MAP_TYPE
//    case _: StructType => TTypeId.STRUCT_TYPE
//    case other =>
//      throw new IllegalArgumentException(s"Unrecognized type name: ${other.catalogString}")
//  }

  def toArrowType(
      ttype: TTypeId,
      jdbcColumnAttributes: Option[JdbcColumnAttributes],
      timeZoneId: String): ArrowType = ttype match {
    case TTypeId.NULL_TYPE => ArrowType.Null.INSTANCE
    case TTypeId.BOOLEAN_TYPE => ArrowType.Bool.INSTANCE
    case TTypeId.TINYINT_TYPE => new ArrowType.Int(8, true)
    case TTypeId.SMALLINT_TYPE => new ArrowType.Int(8 * 2, true)
    case TTypeId.INT_TYPE => new ArrowType.Int(8 * 4, true)
    case TTypeId.BIGINT_TYPE => new ArrowType.Int(8 * 8, true)
    case TTypeId.FLOAT_TYPE => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case TTypeId.DOUBLE_TYPE => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case TTypeId.STRING_TYPE => ArrowType.Utf8.INSTANCE
    case TTypeId.DECIMAL_TYPE if jdbcColumnAttributes.isDefined =>
      val (precision, scale) = (jdbcColumnAttributes.get.precision, jdbcColumnAttributes.get.scale)
      ArrowType.Decimal.createDecimal(precision, scale, null)
    case TTypeId.DATE_TYPE => new ArrowType.Date(DateUnit.DAY)
    case TTypeId.TIMESTAMP_TYPE if timeZoneId == null =>
      throw new IllegalStateException("Missing timezoneId where it is mandatory.")
    case TTypeId.TIMESTAMP_TYPE => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
    case TTypeId.BINARY_TYPE => ArrowType.Binary.INSTANCE
    case TTypeId.INTERVAL_DAY_TIME_TYPE => new ArrowType.Duration(TimeUnit.MICROSECOND)
    case TTypeId.INTERVAL_YEAR_MONTH_TYPE => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
    case other =>
      throw new IllegalArgumentException(s"Unrecognized type name: ${other.name()}")
  }
  // todo: support more types.

//  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
//  def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
//    case BooleanType => ArrowType.Bool.INSTANCE
//    case ByteType => new ArrowType.Int(8, true)
//    case ShortType => new ArrowType.Int(8 * 2, true)
//    case IntegerType => new ArrowType.Int(8 * 4, true)
//    case LongType => new ArrowType.Int(8 * 8, true)
//    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
//    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
//    case StringType => ArrowType.Utf8.INSTANCE
//    case BinaryType => ArrowType.Binary.INSTANCE
//    case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
//    case DateType => new ArrowType.Date(DateUnit.DAY)
//    case TimestampType if timeZoneId == null =>
//      throw new IllegalStateException("Missing timezoneId where it is mandatory.")
//    case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
//    case TimestampNTZType =>
//      new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
//    case NullType => ArrowType.Null.INSTANCE
//    case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
//    case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
//    case _ =>
//      throw QueryExecutionErrors.unsupportedDataTypeError(dt.catalogString)
//  }
//
//  def fromArrowType(dt: ArrowType): DataType = dt match {
//    case ArrowType.Bool.INSTANCE => BooleanType
//    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
//    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
//    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
//    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
//    case float: ArrowType.FloatingPoint
//      if float.getPrecision() == FloatingPointPrecision.SINGLE => FloatType
//    case float: ArrowType.FloatingPoint
//      if float.getPrecision() == FloatingPointPrecision.DOUBLE => DoubleType
//    case ArrowType.Utf8.INSTANCE => StringType
//    case ArrowType.Binary.INSTANCE => BinaryType
//    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
//    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
//    case ts: ArrowType.Timestamp
//      if ts.getUnit == TimeUnit.MICROSECOND && ts.getTimezone == null => TimestampNTZType
//    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
//    case ArrowType.Null.INSTANCE => NullType
//    case yi: ArrowType.Interval if yi.getUnit == IntervalUnit.YEAR_MONTH => YearMonthIntervalType()
//    case di: ArrowType.Duration if di.getUnit == TimeUnit.MICROSECOND => DayTimeIntervalType()
//    case _ => throw QueryExecutionErrors.unsupportedDataTypeError(dt.toString)
//  }
//
  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(
      name: String,
      tpe: TTypeId,
      columnAttributes: Option[JdbcColumnAttributes],
      timeZoneId: String): Field = {
//    dt match {
//      case ArrayType(elementType, containsNull) =>
//        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
//        new Field(name, fieldType,
//          Seq(toArrowField("element", elementType, containsNull, timeZoneId)).asJava)
//      case StructType(fields) =>
//        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
//        new Field(name, fieldType,
//          fields.map { field =>
//            toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
//          }.toSeq.asJava)
//      case MapType(keyType, valueType, valueContainsNull) =>
//        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
//        // Note: Map Type struct can not be null, Struct Type key field can not be null
//        new Field(name, mapType,
//          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
//            new StructType()
//              .add(MapVector.KEY_NAME, keyType, nullable = false)
//              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
//            nullable = false,
//            timeZoneId)).asJava)
//      case dataType =>
//        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null)
//        new Field(name, fieldType, Seq.empty[Field].asJava)
//    }
    // todo:(fchen) improve
    val nullable = true
    val fieldType = new FieldType(nullable, toArrowType(tpe, columnAttributes, timeZoneId), null)
    new Field(name, fieldType, Seq.empty[Field].asJava)
  }
}
