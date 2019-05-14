/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.spark

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SparkSession

/**
 * A recorder for how many client sessions have been cloned by the original [[SparkSession]], which
 * helps the [[SparkSessionCacheManager]] cache and recycle [[SparkSession]] instances.
 *
 * @param spark the original [[SparkSession]] instances
 * @param times times of the original [[SparkSession]] instance has been cloned, start from 1
 * @param initTime Start time of the SparkSession
 */
case class SparkSessionCache private (spark: SparkSession, times: AtomicInteger, initTime: Long)

object SparkSessionCache {
  def apply(spark: SparkSession): SparkSessionCache =
    new SparkSessionCache(spark, new AtomicInteger(1), System.currentTimeMillis)
}
