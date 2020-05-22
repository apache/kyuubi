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

package org.apache.kyuubi

class KyuubiConf {

}

object KyuubiConf {
  /** a custom directory that contains the [[KYUUBI_CONF_FILE_NAME]] */
  final val KYUUBI_CONF_DIR = "KYUUBI_CONF_DIR"
  /** the default file that contains kyuubi properties */
  final val KYUUBI_CONF_FILE_NAME = "kyuubi-defaults.conf"
  final val KYUUBI_HOME = "KYUUBI_HOME"
}
