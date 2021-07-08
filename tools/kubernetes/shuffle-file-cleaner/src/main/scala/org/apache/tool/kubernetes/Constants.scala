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

package org.apache.tool.kubernetes

object Constants {
  val CACHE_DIRS_KEY = "cacheDirs"
  val FILE_EXPIRED_TIME_KEY = "fileExpiredTime"
  val FREE_SPACE_THRESHOLD_KEY = "freeSpaceThreshold"
  val SLEEP_TIME_KEY = "sleepTime"
  val DEEP_CLEAN_FILE_EXPIRED_TIME_KEY = "deepCleanFileExpiredTime"

  val FILE_EXPIRED_TIME_DEFAULT_VALUE = "604800000"
  val FREE_SPACE_THRESHOLD_DEFAULT_VALUE = "60"
  val SLEEP_TIME_DEFAULT_VALUE = "3600000"
  val DEEP_CLEAN_FILE_EXPIRED_TIME_DEFAULT_VALUE = "432000000"
}
