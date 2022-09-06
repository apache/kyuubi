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

package org.apache.kyuubi.operation

import java.util.Locale

import org.apache.kyuubi.KyuubiException

sealed trait PlanOnlyMode {

  /**
   * String name of the plan only mode.
   */
  def name: String
}

case object ParseMode extends PlanOnlyMode { val name = "parse" }

case object AnalyzeMode extends PlanOnlyMode { val name = "analyze" }

case object OptimizeMode extends PlanOnlyMode { val name = "optimize" }

case object OptimizeWithStatsMode extends PlanOnlyMode { val name = "optimize_with_stats" }

case object PhysicalMode extends PlanOnlyMode { val name = "physical" }

case object ExecutionMode extends PlanOnlyMode { val name = "execution" }

case object NoneMode extends PlanOnlyMode { val name = "none" }

case object UnknownMode extends PlanOnlyMode {
  var name = "unknown"

  def mode(mode: String): UnknownMode.type = {
    name = mode
    this
  }
}

object PlanOnlyMode {

  /**
   * Returns the plan only mode from the given string.
   */
  def fromString(mode: String): PlanOnlyMode = mode.toLowerCase(Locale.ROOT) match {
    case ParseMode.name => ParseMode
    case AnalyzeMode.name => AnalyzeMode
    case OptimizeMode.name => OptimizeMode
    case OptimizeWithStatsMode.name => OptimizeWithStatsMode
    case PhysicalMode.name => PhysicalMode
    case ExecutionMode.name => ExecutionMode
    case NoneMode.name => NoneMode
    case other => UnknownMode.mode(other)
  }

  def unknownModeError(mode: PlanOnlyMode): KyuubiException = {
    new KyuubiException(s"Unknown planOnly mode: ${mode.name}. Accepted " +
      "planOnly modes are 'parse', 'analyze', 'optimize', 'optimize_with_stats', 'physical', " +
      "execution, none.")
  }

  def notSupportedModeError(mode: PlanOnlyMode, engine: String): KyuubiException = {
    new KyuubiException(s"The operation mode ${mode.name} doesn't support in $engine engine.")
  }
}

sealed trait PlanOnlyStyle {

  /**
   * String name of the plan only style.
   */
  def name: String
}

case object PlainStyle extends PlanOnlyStyle { val name = "plain" }

case object JsonStyle extends PlanOnlyStyle { val name = "json" }

case object UnknownStyle extends PlanOnlyStyle {
  var name = "unknown"

  def style(style: String): UnknownStyle.type = {
    name = style
    this
  }
}

object PlanOnlyStyle {

  /**
   * Returns the plan only style from the given string.
   */
  def fromString(style: String): PlanOnlyStyle = style.toLowerCase(Locale.ROOT) match {
    case PlainStyle.name => PlainStyle
    case JsonStyle.name => JsonStyle
    case other => UnknownStyle.style(other)
  }

  def unknownStyleError(style: PlanOnlyStyle): KyuubiException = {
    new KyuubiException(s"Unknown planOnly style: ${style.name}. Accepted " +
      "planOnly styles are 'plain', 'json'.")
  }

  def notSupportedStyleError(style: PlanOnlyStyle, engine: String): KyuubiException = {
    new KyuubiException(s"The plan only style ${style.name} doesn't support in $engine engine.")
  }
}
