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

package org.apache.kyuubi.engine.spark.udf

import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * A wrapper for Spark' [[UserDefinedFunction]]
 *
 * @param name function name
 * @param udf user-defined function
 * @param description function description
 */
case class KyuubiDefinedFunction(
    name: String,
    udf: UserDefinedFunction,
    description: String,
    returnType: String,
    since: String)
