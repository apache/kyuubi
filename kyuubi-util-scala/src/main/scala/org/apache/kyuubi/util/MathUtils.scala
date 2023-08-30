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
package org.apache.kyuubi.util

object MathUtils {

  /**
   * Check if the given object represents a positive number in value
   */
  def isPositiveNumber(o: Any): Boolean =
    try {
      o match {
        case intValue: Int => intValue > 0
        case longValue: Long => longValue > 0L
        case floatValue: Float => floatValue > 0.0f
        case doubleValue: Double => doubleValue > 0.0d
        case byteValue: Byte => byteValue > 0
        case shortValue: Short => shortValue > 0
        case charValue: Char => charValue > 0
        case booleanValue: Boolean => booleanValue
        case stringValue: String => stringValue.toDouble > 0.0d
      }
    } catch {
      case t: Throwable =>
        throw new IllegalArgumentException(s"Cannot check if '$o' is a positive number.", t)
    }

  /**
   * Check if the given object represents a non-negative number in value
   */
  def isNonNegativeNumber(o: Any): Boolean =
    try {
      o match {
        case intValue: Int => intValue >= 0
        case longValue: Long => longValue >= 0L
        case floatValue: Float => floatValue >= 0.0f
        case doubleValue: Double => doubleValue >= 0.0d
        case byteValue: Byte => byteValue >= 0
        case shortValue: Short => shortValue >= 0
        case _: Char => true
        case _: Boolean => true
        case stringValue: String => stringValue.toDouble >= 0.0d
      }
    } catch {
      case t: Throwable =>
        throw new IllegalArgumentException(s"Cannot check if '$o' is a non-negative number.", t)
    }
}
