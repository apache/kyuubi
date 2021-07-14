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

package org.apache.kyuubi.ha.v2.strategies

import java.util.Random

import org.apache.kyuubi.ha.v2.{InstanceProvider, ProviderStrategy}

class RandomStrategy[T] extends ProviderStrategy[T] {

  final private val random: Random = new Random

  override def getInstance(instanceProvider: InstanceProvider[T]): Option[T] = {
    val instances = instanceProvider.getInstances
    if (instances.size == 0) return None
    val thisIndex: Int = random.nextInt(instances.size)
    Some(instances(thisIndex))
  }

}

object RandomStrategy {

  def apply[T](): RandomStrategy[T] = new RandomStrategy()

}
