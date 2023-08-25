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

package org.apache.kyuubi.config

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Utils

object ConfigHelpers {

  def strToSeq[T](str: String, converter: String => T, sp: String): Seq[T] = {
    Utils.strToSeq(str, sp).map(converter)
  }

  def strToSet[T](str: String, converter: String => T, sp: String, skipBlank: Boolean): Set[T] = {
    Utils.strToSeq(str, sp).filter(!skipBlank || StringUtils.isNotBlank(_)).map(converter).toSet
  }

  def iterableToStr[T](v: Iterable[T], stringConverter: T => String, sp: String = ","): String = {
    v.map(stringConverter).mkString(sp)
  }
}
