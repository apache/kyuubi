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
package org.apache.kyuubi.engine.jdbc.impala

import org.apache.kyuubi.engine.jdbc.schema.SchemaHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

class ImpalaSchemaHelper extends SchemaHelper {
  override protected def floatToTTypeId: TTypeId = {
    TTypeId.DOUBLE_TYPE
  }

  override protected def realToTTypeId: TTypeId = {
    TTypeId.DOUBLE_TYPE
  }

  // Apache Impala HS2 GetColumns labels the schema column "TABLE_MD" instead of the
  // JDBC-spec "TABLE_SCHEM" (Impala upstream issue, not Kyuubi's). Rewrite the label so
  // Hive JDBC clients see the spec-compliant name. Verified against Impala 4.0.0 impalad:
  // getColumns metadata reports `TABLE_MD` at the position where JDBC requires
  // `TABLE_SCHEM`. Row data is positional, so renaming the column descriptor is sufficient -
  // clients calling rs.getString("TABLE_SCHEM") see the right value.
  override def normalizeMetadataColumnLabel(label: String): String =
    if (label != null && label.equalsIgnoreCase("TABLE_MD")) "TABLE_SCHEM" else label
}
