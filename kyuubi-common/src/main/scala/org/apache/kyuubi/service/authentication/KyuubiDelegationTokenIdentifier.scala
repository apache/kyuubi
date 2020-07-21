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

package org.apache.kyuubi.service.authentication

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

case class KyuubiDelegationTokenIdentifier(
    owner: Text, renewer: Text, realUser: Text) extends AbstractDelegationTokenIdentifier {
  override def getKind: Text = KyuubiDelegationTokenIdentifier.KIND
}

object KyuubiDelegationTokenIdentifier {

  def apply(): KyuubiDelegationTokenIdentifier = {
    KyuubiDelegationTokenIdentifier(new Text(), new Text(), new Text())
  }

  final val KIND = new Text("KYUUBI_DELEGATION_TOKEN")
}
