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

package org.apache.kyuubi.spark.connector.hive.read

import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan

/**
 * No-op stub for Spark 3.3 / 3.4 where
 * [[org.apache.spark.sql.connector.read.Scan.ColumnarSupportMode]]
 * is not yet available (introduced in Spark 3.5.0 by SPARK-44505).
 * The real override that prevents the plan-stage full-table HDFS
 * listing lives in `src/main/scala-spark-3.5-plus/`.
 */
trait KyuubiOrcColumnarMixin { this: OrcScan => }
