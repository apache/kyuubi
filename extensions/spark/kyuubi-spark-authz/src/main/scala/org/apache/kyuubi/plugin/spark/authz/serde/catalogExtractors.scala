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

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

trait DatabaseExtractor extends (AnyRef => String) with Extractor

object DatabaseExtractor {
  val dbExtractors: Map[String, DatabaseExtractor] = {
    ServiceLoader.load(classOf[DatabaseExtractor])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }
}

/**
 * String
 */
class StringDatabaseExtractor extends DatabaseExtractor {
  override def apply(v1: AnyRef): String = {
    v1.asInstanceOf[String]
  }
}

/**
 * Option[String]
 */
class StringOptionDatabaseExtractor extends DatabaseExtractor {
  override def apply(v1: AnyRef): String = {
    v1.asInstanceOf[Option[String]].orNull
  }
}

/**
 * Seq[String]
 */
class StringSeqDatabaseExtractor extends DatabaseExtractor {
  override def apply(v1: AnyRef): String = {
    quote(v1.asInstanceOf[Seq[String]])
  }
}

/**
 * Option[Seq[String]]
 */
class StringSeqOptionDatabaseExtractor extends DatabaseExtractor {
  override def apply(v1: AnyRef): String = {
    v1.asInstanceOf[Option[Seq[String]]].map(quote).orNull
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
 */
class ResolvedNamespaceDatabaseExtractor extends DatabaseExtractor {
  override def apply(v1: AnyRef): String = {
    val namespace = getFieldVal[Seq[String]](v1, "namespace")
    quote(namespace)
  }
}

class ResolvedDBObjectNameDatabaseExtractor extends DatabaseExtractor {
  override def apply(v1: AnyRef): String = {
    val namespace = getFieldVal[Seq[String]](v1, "nameParts")
    quote(namespace)
  }
}
