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

import org.apache.spark.sql.connector.catalog.{CatalogPlugin, TableCatalog}

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.invokeAs

trait CatalogExtractor extends ((AnyRef) => Option[String]) with Extractor

class DataSourceV2RelationCatalogExtractor extends CatalogExtractor {
  override def apply(v2: AnyRef): Option[String] = {
    val maybeCatalog = invokeAs[Option[CatalogPlugin]](v2, "catalog")
    maybeCatalog match {
      case None => None
      case Some(catalogPlugin) => Some(catalogPlugin.name())
    }
  }
}

class ResolvedTableCatalogExtractor extends CatalogExtractor {
  override def apply(v1: AnyRef): Option[String] = {
    val catalog = invokeAs[TableCatalog](v1, "catalog")
    Some(catalog.name())
  }
}
