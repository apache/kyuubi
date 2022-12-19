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

package org.apache.kyuubi.sql.schema

case class Row(values: List[_]) {

  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  def get(i: Int): Any = {
    values(i)
  }

  def length: Int = values.length

  /** Checks whether the value at position i is null. */
  def isNullAt(i: Int): Boolean = get(i) == null

  /**
   * Returns the value at position i as a primitive boolean.
   *
   * @throws ClassCastException   when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getBoolean(i: Int): Boolean = getAs[Boolean](i)

  /**
   * Returns the value at position i as a primitive byte.
   *
   * @throws ClassCastException   when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getByte(i: Int): Byte = getAs[Byte](i)

  /**
   * Returns the value at position i as a primitive short.
   *
   * @throws ClassCastException   when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getShort(i: Int): Short = getAs[Short](i)

  /**
   * Returns the value at position i as a primitive int.
   *
   * @throws ClassCastException   when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getInt(i: Int): Int = getAs[Int](i)

  /**
   * Returns the value at position i as a primitive long.
   *
   * @throws ClassCastException   when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getLong(i: Int): Long = getAs[Long](i)

  /**
   * Returns the value at position i as a primitive float.
   * Throws an exception if the type mismatches or if the value is null.
   *
   * @throws ClassCastException   when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getFloat(i: Int): Float = getAs[Float](i)

  /**
   * Returns the value at position i as a primitive double.
   *
   * @throws ClassCastException   when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getDouble(i: Int): Double = getAs[Double](i)

  /**
   * Returns the value at position i as a String object.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getString(i: Int): String = getAs[String](i)

  /**
   * Returns the value at position i of decimal type as java.math.BigDecimal.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getDecimal(i: Int): java.math.BigDecimal = getAs[java.math.BigDecimal](i)

}
