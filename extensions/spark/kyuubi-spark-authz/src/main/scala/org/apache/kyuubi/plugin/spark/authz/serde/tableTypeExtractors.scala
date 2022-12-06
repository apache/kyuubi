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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.kyuubi.plugin.spark.authz.serde.TableType.TableType

object TableType extends Enumeration {
  type TableType = Value
  val TABLE, PERMANENT_VIEW, TEMP_VIEW, GLOBAL_TEMP_VIEW = Value
}

trait TableTypeExtractor extends ((AnyRef, SparkSession) => TableType) with Extractor

object TableTypeExtractor {
  val tableTypeExtractors: Map[String, TableTypeExtractor] = {
    ServiceLoader.load(classOf[TableTypeExtractor])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ViewType
 */
class ViewTypeTableTypeExtractor extends TableTypeExtractor {
  override def apply(v1: AnyRef, spark: SparkSession): TableType = {
    v1.toString match {
      case "LocalTempView" => TableType.TEMP_VIEW
      case "GlobalTempView" => TableType.GLOBAL_TEMP_VIEW
      case _ => TableType.PERMANENT_VIEW
    }
  }
}

class TableIdentifierTableTypeExtractor extends TableTypeExtractor {
  override def apply(v1: AnyRef, spark: SparkSession): TableType = {
    val catalog = spark.sessionState.catalog
    val identifier = v1.asInstanceOf[TableIdentifier]
    val nameParts: Seq[String] = identifier.database.toSeq :+ identifier.table
    if (catalog.isTempView(nameParts)) {
      TableType.TEMP_VIEW
    } else {
      TableType.PERMANENT_VIEW
    }
  }
}
