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

package org.apache.kyuubi.sql.zorder

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * TODO: shall we forbid zorder if it's dynamic partition inserts ?
 * Insert zorder before writing datasource if the target table properties has zorder properties
 */
case class InsertZorderBeforeWritingDatasource(session: SparkSession)
  extends InsertZorderBeforeWritingDatasourceBase {
  override def buildZorder(children: Seq[Expression]): ZorderBase = Zorder(children)
}

/**
 * TODO: shall we forbid zorder if it's dynamic partition inserts ?
 * Insert zorder before writing hive if the target table properties has zorder properties
 */
case class InsertZorderBeforeWritingHive(session: SparkSession)
  extends InsertZorderBeforeWritingHiveBase {
  override def buildZorder(children: Seq[Expression]): ZorderBase = Zorder(children)
}
