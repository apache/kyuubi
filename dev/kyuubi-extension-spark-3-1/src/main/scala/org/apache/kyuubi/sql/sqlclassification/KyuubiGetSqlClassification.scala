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

package org.apache.kyuubi.sql.sqlclassification

import java.net.URL

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.KyuubiSQLConf._

/**
 * This object is used for getting sql_classification by the logicalPlan's simpleName.
 * When the configuration item: SQL_CLASSIFICATION_ENABLED is on,
 * we will load the rule from sql-classification-default.json.
 *
 * Notice:
 *  We support the user use the self-defined matching rule: sql-classification.json.
 *  If there have no this named jsonFile,
 *  the service will upload the default matching rule: sql-classification-default.json.
 */
object KyuubiGetSqlClassification extends Logging {
  private val jsonNode: Option[JsonNode] = {
    SQLConf.get.getConf(SQL_CLASSIFICATION_ENABLED) match {
      case true =>
        val objectMapper = new ObjectMapper
        var url: URL = getClass.getClassLoader.getResource("sql-classification.json")
        if (url == null) {
          logInfo("sql-classification.json is not found, use default config instead")
          url = getClass.getClassLoader.getResource("sql-classification-default.json")
        }
        Some(objectMapper.readTree(url))
      case false =>
        None
    }
  }

  /**
   * Notice:
   *    You need to make sure that the configuration item: SQL_CLASSIFICATION_ENABLED
    *   is true
   * @param simpleName: the analyzied_logical_plan's getSimpleName
   * @return: This sql's classification
   */
  def getSqlClassification(simpleName: String): String = {
    jsonNode.map { json =>
      val sqlClassififation = json.get(simpleName)
      if (sqlClassififation == null) {
        "dql"
      } else {
        sqlClassififation.asText()
      }
    }.getOrElse(
      throw new IllegalArgumentException(
        s"You should restart engine with: ${SQL_CLASSIFICATION_ENABLED.key} true")
    )
  }
}
