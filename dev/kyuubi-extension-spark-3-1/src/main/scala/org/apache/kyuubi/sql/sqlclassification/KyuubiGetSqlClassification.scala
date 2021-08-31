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

import java.io.File

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.KyuubiSQLConf._

/**
 * This object is used for getting sql_classification by the logicalPlan's simpleName.
 * When the configuration item: SQL_CLASSIFICATION_ENABLED is on,
 * we will load the rule from sql-classification-default.json.
 */
object KyuubiGetSqlClassification {

  private val enabled: Boolean = SQLConf.get.getConf(SQL_CLASSIFICATION_ENABLED)
  private var jsonNode: JsonNode = null
  if (enabled) {
    try {
      val defaultSqlClassificationFile =
        this.getClass.getClassLoader.getResource("sql-classification-default.json").getPath
      val objectMapper = new ObjectMapper
      jsonNode = objectMapper.readTree(new File(defaultSqlClassificationFile))
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException("sql-classification-default.json is not exist")
    }
  }

  def getSqlClassification(simpleName: String): String = {

    val iteratorSqlClassification = jsonNode.fields()
    while (iteratorSqlClassification.hasNext) {
      val item = iteratorSqlClassification.next()
      val sqlClassififation = item.getKey
      val simpleNameList = item.getValue

      val nameIterator = simpleNameList.iterator()
      while (nameIterator.hasNext) {
        if (simpleName.equals(nameIterator.next().asText())) {
          return sqlClassififation
        }
      }
    }
    return "others"
  }
}
