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
package org.apache.kyuubi.plugin.spark.authz.ranger

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

class FilterDataSourceV2Strategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case p: LogicalPlan if p.nodeName == "ShowNamespaces" =>
      val clazz = Class
        .forName("org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces")
      val namespace = getFieldValue(clazz, "namespace", p)
      val pattern = getFieldValue(clazz, "pattern", p)
      val output = getFieldValue(clazz, "output", p)

      val resolvedNamespaceClazz = Class
        .forName("org.apache.spark.sql.catalyst.analysis.ResolvedNamespace")
      val catalog = getFieldValue(resolvedNamespaceClazz, "catalog", namespace)
      val resolvedNamespace = getFieldValue(resolvedNamespaceClazz, "namespace", namespace)

      val showNamespaceExecClazz = Class
        .forName("org.apache.spark.sql.execution.datasources.v2.ShowNamespacesExec")
      val constructor = showNamespaceExecClazz.getConstructor(
        classOf[Seq[Attribute]],
        Class.forName("org.apache.spark.sql.connector.catalog.SupportsNamespaces"),
        classOf[Seq[String]],
        classOf[Option[String]])

      val showNamespacesExec = constructor.newInstance(output, catalog, resolvedNamespace, pattern)
        .asInstanceOf[SparkPlan]
      FilterShowNamespaceExec(showNamespacesExec) :: Nil
    case _ => Nil
  }

  private def getFieldValue(clazz: Class[_], fieldName: String, obj: Object): AnyRef = {
    val field = clazz.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(obj)
  }
}
