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

package yaooqinn.kyuubi

import scala.collection.mutable.HashMap

import org.apache.hadoop.fs.permission.FsPermission

package object yarn {

  type EnvMap = HashMap[String, String]

  val KYUUBI_YARN_APP_NAME = "KYUUBI SERVER"
  val KYUUBI_YARN_APP_TYPE = "KYUUBI"
  // Staging directory for any temporary jars or files
  val KYUUBI_STAGING: String = ".kyuubiStaging"
  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)
  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  val SPARK_CONF_DIR = "__spark_conf__"
  val SPARK_CONF_FILE = "__spark_conf__.properties"
  // Subdirectory in the conf directory containing Hadoop config files.
  val HADOOP_CONF_DIR = "__hadoop_conf__"
  // File containing the conf archive in the AM. See prepareLocalResources().
  val SPARK_CONF_ARCHIVE: String = SPARK_CONF_DIR + ".zip"
  val SPARK_LIB_DIR = "__spark_libs__"
  val LOCAL_SCHEME = "local"
}
