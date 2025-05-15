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

package org.apache.kyuubi.engine.spark.repl

import java.util.HashMap

import org.apache.spark.kyuubi.SparkContextHelper
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Helper class to wrap a [[DataFrame]] and pass its results to clients
 *
 * @since 1.5.0
 */
class DataFrameHolder(spark: SparkSession) {

  private val results = new HashMap[String, DataFrame]()

  private def currentId: String = {
    SparkContextHelper.getCurrentStatementId(spark.sparkContext)
  }

  /**
   * Set Results
   * @param df a DataFrame for pass result to to clients
   */
  def set(df: DataFrame): Unit = {
    results.put(currentId, df)
  }

  /**
   * Get Result
   * @param statementId kyuubi statement id
   */
  def get(statementId: String): DataFrame = {
    results.get(statementId)
  }

  /**
   * Clear Result
   * @param statementId kyuubi statement id
   */
  def unset(statementId: String): Unit = {
    results.remove(statementId)
  }
}
