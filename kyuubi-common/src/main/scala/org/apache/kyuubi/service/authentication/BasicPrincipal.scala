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

import java.security.Principal
import java.util.Objects

class BasicPrincipal(val name: String) extends Principal {
  require(name != null, "Principal name cannot be null")

  override def getName: String = name

  override def toString: String = name

  override def equals(o: Any): Boolean = {
    if (this == o) {
      true
    } else if (o == null || getClass != o.getClass) {
      false
    } else {
      Objects.equals(name, o.asInstanceOf[BasicPrincipal].name)
    }
  }

  override def hashCode: Int = Objects.hashCode(name)
}
