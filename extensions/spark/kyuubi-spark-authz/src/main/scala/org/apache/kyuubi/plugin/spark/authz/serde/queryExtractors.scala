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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait QueryExtractor extends (AnyRef => LogicalPlan) with Extractor

object QueryExtractor {
  val queryExtractors: Map[String, QueryExtractor] = {
    ServiceLoader.load(classOf[QueryExtractor])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }
}

class LogicalPlanQueryExtractor extends QueryExtractor {
  override def apply(v1: AnyRef): LogicalPlan = {
    v1.asInstanceOf[LogicalPlan]
  }
}

class LogicalPlanOptionQueryExtractor extends QueryExtractor {
  override def apply(v1: AnyRef): LogicalPlan = {
    v1.asInstanceOf[Option[LogicalPlan]].orNull
  }
}
