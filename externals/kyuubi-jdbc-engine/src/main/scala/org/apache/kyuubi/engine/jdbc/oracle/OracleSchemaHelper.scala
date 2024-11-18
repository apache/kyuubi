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
package org.apache.kyuubi.engine.jdbc.oracle

import java.sql.Types

import org.apache.kyuubi.engine.jdbc.schema.SchemaHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeDesc

class OracleSchemaHelper extends SchemaHelper {
  override protected def toTTypeDesc(sqlType: Int, precision: Int, scale: Int): TTypeDesc = {
    sqlType match {
      case Types.NUMERIC if scale == 0 =>
        super.toTTypeDesc(Types.INTEGER, precision, scale)
      case Types.NUMERIC =>
        super.toTTypeDesc(Types.DECIMAL, precision, scale)
      case _ => super.toTTypeDesc(sqlType, precision, scale)
    }
  }
}
