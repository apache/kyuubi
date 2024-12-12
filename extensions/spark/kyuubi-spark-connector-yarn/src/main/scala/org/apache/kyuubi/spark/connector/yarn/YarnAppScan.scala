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

package org.apache.kyuubi.spark.connector.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.kyuubi.spark.connector.yarn.YarnAppScan.resolvePlaceholders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class YarnAppScan(options: CaseInsensitiveStringMap, schema: StructType) extends ScanBuilder
  with Scan with Batch with Serializable {
  private var hadoopConfMap: Map[String, String] = _

  override def toBatch: Batch = this

  private val appId: String = options.getOrDefault("appId", "*")
  private val _options: CaseInsensitiveStringMap = options
  // private val yarnApplication: YarnApplication = new YarnApplication(id =
  // options.get("appId").isEmpty
  // )
  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    // Fetch app for the given appId (filtering logic can be added)
    // hadoopConf can not be serialized correctly
    // use map here
    Array(new YarnAppPartition(appId, hadoopConfMap))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new YarnAppReaderFactory

  override def build(): Scan = {
    val hadoopConf = SparkSession.active.sparkContext.hadoopConfiguration
    hadoopConfMap = resolvePlaceholders(hadoopConf)
    this
  }
}

object YarnAppScan {
  /**
   * resolve Hadoop Configuration ，replace "${xxx}" with the value of xxx
   *
   * for example
   * input Configuration, where key1=value1 and key2=${key1}
   * output Map(key1 -> value1, key2 -> value1)
   *
   * @param inputConf the input Configuration
   * @return resolved Map
   */
  def resolvePlaceholders(inputConf: Configuration): Map[String, String] = {
    import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
    val conf = inputConf.asScala.map(entry => entry.getKey -> entry.getValue).toMap

    def resolve(value: String, visitedKeys: Set[String]): String = {
      val placeholderPattern = "\\$\\{([^}]+)\\}".r

      placeholderPattern.replaceAllIn(value, m => {
        val key = m.group(1)
        if (visitedKeys.contains(key)) {
          throw new IllegalArgumentException(s"Circular reference detected for key: $key")
        }
        conf.get(key) match {
          case Some(replacement) => resolve(replacement, visitedKeys + key)
          case None => resolve("", visitedKeys + key)
        }
      })
    }

    conf.map { case (key, value) =>
      key -> resolve(value, Set(key))
    }
  }
}
