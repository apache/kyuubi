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

package org.apache.kyuubi.plugin.lineage

import scala.util.{Failure, Success, Try}

import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.isSparkVersionAtLeast

class LineagePermanentViewMarkerRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (SparkContextHelper.getConf(LineageConf.SKIP_PARSING_PERMANENT_VIEW_ENABLED)) {
      plan mapChildren {
        case p: LineagePermanentViewMarker => p
        case permanentView: View if hasResolvedPermanentView(permanentView) =>
          LineagePermanentViewMarker(permanentView, permanentView.desc)
        case other => apply(other)
      }
    } else plan
  }

  def hasResolvedPermanentView(plan: LogicalPlan): Boolean = {
    plan match {
      case view: View if view.resolved && isSparkVersionAtLeast("3.1.0") =>
        !getFieldVal[Boolean](view, "isTempView")
      case _ =>
        false
    }
  }

  private def getFieldVal[T](o: Any, name: String): T = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredFields.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in $candidates", e)
    }
  }
}
