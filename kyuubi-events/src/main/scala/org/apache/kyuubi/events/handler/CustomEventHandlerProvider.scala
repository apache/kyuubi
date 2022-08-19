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
package org.apache.kyuubi.events.handler

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.KyuubiEvent

/**
 * Custom EventHandler provider. We can implement it to provide a custom EventHandler.
 * The implementation will be loaded by ``Service Provider Interface``.
 */
trait CustomEventHandlerProvider {

  /**
   * The create method is called to create a custom event handler
   * when this implementation is loaded.
   *
   * @param kyuubiConf The conf can be used to read some configs.
   * @return A custom handler to handle KyuubiEvent.
   */
  def create(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent]
}
