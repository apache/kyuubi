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

package org.apache.kyuubi.plugin.spark.authz.util

import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier

/**
 * An object for handling table access through delta.`/some/path`. This is a stop-gap solution
 * until PathIdentifiers are implemented in Apache Spark. Borrowed from class
 * org.apache.spark.sql.delta.catalog.SupportsPathIdentifier of Delta Lake.
 */
private[authz] object PathIdentifier {

  val spark = SparkSession.active

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  private def isDeltaDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == "delta"
  }

  private def hasDeltaNamespace(namespace: Seq[String]): Boolean = {
    namespace.length == 1 && isDeltaDataSourceName(namespace.head)
  }

  def isPathIdentifier(namespace: Seq[String], table: String): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasDeltaNamespace(namespace) && new Path(table).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  def isPathIdentifier(ident: Identifier): Boolean = {
    isPathIdentifier(ident.namespace(), ident.name())
  }
}
