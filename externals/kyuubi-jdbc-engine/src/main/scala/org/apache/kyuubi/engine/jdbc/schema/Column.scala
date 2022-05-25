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
package org.apache.kyuubi.engine.jdbc.schema

/**
 * @param name        The designated column's name.
 * @param typeName    The designated column's database-specific type name.
 * @param sqlType     The designated column's SQL type.
 * @param precision   The designated column's specified column size.
 *                    For numeric data, this is the maximum precision.
 *                    For character data, this is the length in characters.
 *                    For datetime datatypes, this is the length in characters of the
 *                    String representation (assuming the maximum allowed precision of
 *                    the fractional seconds component).
 *                    For binary data, this is the length in bytes. For the ROWID datatype,
 *                    this is the length in bytes. 0 is returned for data types where the
 *                    column size is not applicable.
 * @param scale       The designated column's number of digits to right of the decimal point.
 *                    0 is returned for data types where the scale is not applicable.
 * @param label       The designated column's suggested title for use in printouts and
 *                    displays. The suggested title is usually specified by the SQL <code>AS</code>
 *                    clause.  If a SQL <code>AS</code> is not specified, the value returned will
 *                    be the same as the value returned by the name.
 * @param displaySize Indicates the designated column's normal maximum width in characters.
 */
case class Column(
    name: String,
    typeName: String,
    sqlType: Int,
    precision: Int,
    scale: Int,
    label: String,
    displaySize: Int)
