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

package org.apache.spark.sql.planner

import org.apache.spark.connect.proto.Expression
import org.apache.spark.sql.catalyst.{expressions, CatalystTypeConverters}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput, LiteralValueProtoConverter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object LiteralExpressionProtoConverter {

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * @return
   *   Expression
   */
  def toCatalystExpression(lit: Expression.Literal): expressions.Literal = {
    lit.getLiteralTypeCase match {
      case Expression.Literal.LiteralTypeCase.NULL =>
        expressions.Literal(null, DataTypeProtoConverter.toCatalystType(lit.getNull))

      case Expression.Literal.LiteralTypeCase.BINARY =>
        expressions.Literal(lit.getBinary.toByteArray, BinaryType)

      case Expression.Literal.LiteralTypeCase.BOOLEAN =>
        expressions.Literal(lit.getBoolean, BooleanType)

      case Expression.Literal.LiteralTypeCase.BYTE =>
        expressions.Literal(lit.getByte.toByte, ByteType)

      case Expression.Literal.LiteralTypeCase.SHORT =>
        expressions.Literal(lit.getShort.toShort, ShortType)

      case Expression.Literal.LiteralTypeCase.INTEGER =>
        expressions.Literal(lit.getInteger, IntegerType)

      case Expression.Literal.LiteralTypeCase.LONG =>
        expressions.Literal(lit.getLong, LongType)

      case Expression.Literal.LiteralTypeCase.FLOAT =>
        expressions.Literal(lit.getFloat, FloatType)

      case Expression.Literal.LiteralTypeCase.DOUBLE =>
        expressions.Literal(lit.getDouble, DoubleType)

      case Expression.Literal.LiteralTypeCase.DECIMAL =>
        val decimal = Decimal.apply(lit.getDecimal.getValue)
        var precision = decimal.precision
        if (lit.getDecimal.hasPrecision) {
          precision = math.max(precision, lit.getDecimal.getPrecision)
        }
        var scale = decimal.scale
        if (lit.getDecimal.hasScale) {
          scale = math.max(scale, lit.getDecimal.getScale)
        }
        expressions.Literal(decimal, DecimalType(math.max(precision, scale), scale))

      case Expression.Literal.LiteralTypeCase.STRING =>
        expressions.Literal(UTF8String.fromString(lit.getString), StringType)

      case Expression.Literal.LiteralTypeCase.DATE =>
        expressions.Literal(lit.getDate, DateType)

      case Expression.Literal.LiteralTypeCase.TIMESTAMP =>
        expressions.Literal(lit.getTimestamp, TimestampType)

      case Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ =>
        expressions.Literal(lit.getTimestampNtz, TimestampNTZType)

      case Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL =>
        val interval = new CalendarInterval(
          lit.getCalendarInterval.getMonths,
          lit.getCalendarInterval.getDays,
          lit.getCalendarInterval.getMicroseconds)
        expressions.Literal(interval, CalendarIntervalType)

      case Expression.Literal.LiteralTypeCase.YEAR_MONTH_INTERVAL =>
        expressions.Literal(lit.getYearMonthInterval, YearMonthIntervalType())

      case Expression.Literal.LiteralTypeCase.DAY_TIME_INTERVAL =>
        expressions.Literal(lit.getDayTimeInterval, DayTimeIntervalType())

      case Expression.Literal.LiteralTypeCase.ARRAY =>
        expressions.Literal.create(
          LiteralValueProtoConverter.toCatalystArray(lit.getArray),
          ArrayType(DataTypeProtoConverter.toCatalystType(lit.getArray.getElementType)))

      case Expression.Literal.LiteralTypeCase.MAP =>
        expressions.Literal.create(
          LiteralValueProtoConverter.toCatalystMap(lit.getMap),
          MapType(
            DataTypeProtoConverter.toCatalystType(lit.getMap.getKeyType),
            DataTypeProtoConverter.toCatalystType(lit.getMap.getValueType)))

      case Expression.Literal.LiteralTypeCase.STRUCT =>
        val dataType = DataTypeProtoConverter.toCatalystType(lit.getStruct.getStructType)
        val structData = LiteralValueProtoConverter.toCatalystStruct(lit.getStruct)
        val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
        expressions.Literal(convert(structData), dataType)

      case _ =>
        throw InvalidPlanInput(
          s"Unsupported Literal Type: ${lit.getLiteralTypeCase.getNumber}" +
            s"(${lit.getLiteralTypeCase.name})")
    }
  }
}
