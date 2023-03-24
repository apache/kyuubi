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

function secondTransfer(val: number) {
  const h = Math.floor(val / 3600)
  const min = Math.floor((val - 3600 * h) / 60)
  const sec = Math.round(val - 3600 * h - 60 * min)
  return h === 0
    ? min == 0
      ? `${sec} sec`
      : sec === 0
      ? `${min} min`
      : `${min} min ${sec} sec`
    : sec === 0
    ? min !== 0
      ? `${h} hour ${min} min`
      : `${h} hour`
    : `${h} hour ${min} min ${sec} sec`
}

export { secondTransfer }
