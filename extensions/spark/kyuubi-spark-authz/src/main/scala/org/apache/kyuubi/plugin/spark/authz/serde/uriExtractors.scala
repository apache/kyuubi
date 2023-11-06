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

import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

trait URIExtractor extends (AnyRef => Seq[Uri]) with Extractor

object URIExtractor {
  val uriExtractors: Map[String, URIExtractor] = {
    loadExtractorsToMap[URIExtractor]
  }
}

/**
 * String
 */
class StringURIExtractor extends URIExtractor {
  override def apply(v1: AnyRef): Seq[Uri] = {
    Seq(Uri(v1.asInstanceOf[String]))
  }
}

class CatalogStorageFormatURIExtractor extends URIExtractor {
  override def apply(v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[CatalogStorageFormat].locationUri.map(uri => Uri(uri.getPath)).toSeq
  }
}

class OptionsUriExtractor extends URIExtractor {
  override def apply(v1: AnyRef): Seq[Uri] = {
    v1.asInstanceOf[Map[String, String]].get("path").map(Uri).toSeq
  }
}

class BaseRelationFileIndexURIExtractor extends URIExtractor {
  override def apply(v1: AnyRef): Seq[Uri] = {
    v1 match {
      case h: HadoopFsRelation => h.location.rootPaths.map(_.toString).map(Uri)
      case _ => Nil
    }
  }
}
