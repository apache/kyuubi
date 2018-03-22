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

package yaooqinn.kyuubi.schema

import org.apache.hive.service.cli.thrift.TColumnDesc
import org.apache.spark.sql.types.StructField

class ColumnDescriptor private(name: String, comment: String, typeDesc: TypeDescriptor, pos: Int) {

  def this(filed: StructField, pos: Int) =
    this(filed.name, filed.getComment().getOrElse(""), new TypeDescriptor(filed.dataType), pos)

  def toTColumnDesc: TColumnDesc = {
    val tColumnDesc = new TColumnDesc
    tColumnDesc.setColumnName(name)
    tColumnDesc.setComment(comment)
    tColumnDesc.setTypeDesc(typeDesc.toTTypeDesc)
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }
}
