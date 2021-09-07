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

package org.apache.kyuubi.sql.zorder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

case class Zorder(children: Seq[Expression]) extends Expression with CodegenFallback {
  private lazy val defaultNullValues = {
    children.map {
      case bf: BoundReference =>
        bf.dataType match {
          case BooleanType =>
            false
          case ByteType =>
            Byte.MaxValue
          case ShortType =>
            Short.MaxValue
          case IntegerType =>
            Int.MaxValue
          case LongType =>
            Long.MaxValue
          case FloatType =>
            Float.MaxValue
          case DoubleType =>
            Double.MaxValue
          case StringType =>
            ""
          case TimestampType =>
            Long.MaxValue
          case DateType =>
            Int.MaxValue
          case d: DecimalType =>
            Long.MaxValue
          case other: Any =>
            throw new ZorderException("Unsupported z-order type: " + other.getClass)
        }
      case other: Any =>
        throw new ZorderException("Unknown z-order column: " + other)
    }
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val evaluated = children.zipWithIndex.map { case (child: Expression, index) =>
      val v = child.eval(input)
      if (v == null) {
        defaultNullValues(index)
      } else {
        v
      }
    }

    val binaryArr = evaluated.map(ZorderBytesUtils.toByte).toArray
    ZorderBytesUtils.interleaveMultiByteArray(binaryArr)
  }

  override def dataType: DataType = BinaryType
}
