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

package org.apache.kyuubi.security.hive

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

/**
 * A delegation token identifier that is specific to Hive.
 * @param owner    the effective username of the token owner
 * @param renewer  the username of the renewer
 * @param realUser the real username of the token owner
 */
class DelegationTokenIdentifier(owner: Text, renewer: Text, realUser: Text)
    extends AbstractDelegationTokenIdentifier(owner, renewer, realUser) {

  /**
   * Create an empty delegation token identifier for reading into.
   */
  def this() = this(null, null, null)

  override def getKind: Text = DelegationTokenIdentifier.HIVE_DELEGATION_KIND
}

object DelegationTokenIdentifier {
  val HIVE_DELEGATION_KIND = new Text("HIVE_DELEGATION_TOKEN")
}
