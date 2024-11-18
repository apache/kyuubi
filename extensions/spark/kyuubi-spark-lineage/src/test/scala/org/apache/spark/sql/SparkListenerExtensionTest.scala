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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.Utils

trait SparkListenerExtensionTest {

  protected val catalogImpl: String
  protected def format: String = if (catalogImpl == "hive") "hive" else "parquet"

  protected lazy val spark: SparkSession = {
    val basePath = Utils.createTempDir() + "/" + getClass.getCanonicalName
    val metastorePath = basePath + "/metastore_db"
    val warehousePath = basePath + "/warehouse"
    SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", "false")
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metastorePath;create=true")
      .config("spark.sql.catalogImplementation", catalogImpl)
      .config(StaticSQLConf.WAREHOUSE_PATH.key, warehousePath)
      .config(sparkConf)
      .getOrCreate()
  }

  protected def withTable(t: String*)(f: Seq[String] => Unit): Unit = {
    try {
      f(t)
    } finally {
      t.foreach(x => spark.sql(s"DROP TABLE IF EXISTS $x"))
    }
  }

  protected def withView(t: String*)(f: Seq[String] => Unit): Unit = {
    try {
      f(t)
    } finally {
      t.foreach(x => spark.sql(s"DROP VIEW IF EXISTS $x"))
    }
  }

  def sparkConf(): SparkConf = new SparkConf()

}
