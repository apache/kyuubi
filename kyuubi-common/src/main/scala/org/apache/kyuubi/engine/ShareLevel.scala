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

package org.apache.kyuubi.engine

/**
 * A SQL Engine APP will be shared by different levels
 */
object ShareLevel extends Enumeration {
  type ShareLevel = Value
  val
    /** In this level, An APP will not be shared and used only for a single session */
    CONNECTION,
    /** DEFAULT level, An APP will be shared for all sessions created by a user */
    USER,
    /** In this level, An APP will be shared for all sessions created by a user's default group */
    GROUP,
    /** In this level, All sessions from one or more Kyuubi server's will share one single APP */
    SERVER = Value
}
