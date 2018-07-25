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

package yaooqinn.kyuubi.yarn

import org.apache.hadoop.fs.permission.FsAction
import org.apache.spark.SparkFunSuite

class packageSuite extends SparkFunSuite {

  test("yarn package object test") {
    assert(KYUUBI_YARN_APP_NAME === "KYUUBI SERVER")
    assert(KYUUBI_YARN_APP_TYPE === "KYUUBI")
    assert(KYUUBI_STAGING === ".kyuubiStaging")
    assert(STAGING_DIR_PERMISSION.getUserAction === FsAction.ALL)
    assert(STAGING_DIR_PERMISSION.getGroupAction === FsAction.NONE)
    assert(STAGING_DIR_PERMISSION.getOtherAction === FsAction.NONE)
    assert(APP_FILE_PERMISSION.getUserAction === FsAction.READ_WRITE)
    assert(APP_FILE_PERMISSION.getGroupAction === FsAction.READ)
    assert(APP_FILE_PERMISSION.getOtherAction === FsAction.READ)
    assert(SPARK_CONF_DIR === "__spark_conf__")
    assert(SPARK_CONF_FILE === "__spark_conf__.properties")
    assert(HADOOP_CONF_DIR === "__hadoop_conf__")
    assert(SPARK_CONF_ARCHIVE === SPARK_CONF_DIR + ".zip")
    assert(SPARK_LIB_DIR === "__spark_libs__")
    assert(LOCAL_SCHEME === "local")
  }
}
