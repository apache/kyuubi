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
  // JDBC-spec "TABLE_SCHEM" - an upstream typo. Every other metadata result set in the
  // same file correctly uses TABLE_SCHEM; the row-population site at L494 even comments
  // `// TABLE_SCHEM`. See:
  // scalastyle:off line.size.limit
  //   https://github.com/apache/impala/blob/4.5.0/fe/src/main/java/org/apache/impala/service/MetadataOp.java#L114
  // scalastyle:on line.size.limit
  // Rewrite the label so clients calling rs.getString("TABLE_SCHEM") see the right value.
  // Row data is positional, so renaming the column descriptor alone is sufficient.
  override def normalizeMetadataColumnLabel(label: String): String =
    if (label != null && label.equalsIgnoreCase("TABLE_MD")) "TABLE_SCHEM" else label
}
