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

import os from 'os'

const getLocalhostIP = () => {
  const localhost = 'localhost'
  try {
    const network = os.networkInterfaces()
    for (const dev in network) {
      const iface = network[dev]
      for (let i = 0; iface != undefined && i < iface.length; i++) {
        const alias = iface[i]
        if (
          alias.family === 'IPv4' &&
          alias.address !== '127.0.0.1' &&
          !alias.internal
        ) {
          return alias.address
        }
      }
    }
    return localhost
  } catch (error) {
    // we need this info to debug
    /* eslint-disable no-console */
    console.error(error)
    /* eslint-enable no-console */
    return '0.0.0.0'
  }
}

export default getLocalhostIP
