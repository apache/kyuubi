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

package org.apache.kyuubi.operation

import org.apache.hive.service.rpc.thrift.TFetchOrientation

object FetchOrientation extends Enumeration {
  type FetchOrientation = Value

  val FETCH_NEXT,
      FETCH_PRIOR,
      FETCH_FIRST,
      FETCH_LAST,
      FETCH_RELATIVE,
      FETCH_ABSOLUTE = Value

  def getFetchOrientation(from: TFetchOrientation): FetchOrientation =
    from match {
      case TFetchOrientation.FETCH_FIRST => FETCH_FIRST
      case TFetchOrientation.FETCH_PRIOR => FETCH_PRIOR
      case TFetchOrientation.FETCH_RELATIVE => FETCH_RELATIVE
      case TFetchOrientation.FETCH_ABSOLUTE => FETCH_ABSOLUTE
      case TFetchOrientation.FETCH_LAST => FETCH_LAST
      case _ => FETCH_NEXT
    }

  def toTFetchOrientation(from: FetchOrientation): TFetchOrientation = {
    from match {
      case FETCH_FIRST => TFetchOrientation.FETCH_FIRST
      case FETCH_LAST => TFetchOrientation.FETCH_LAST
      case FETCH_ABSOLUTE => TFetchOrientation.FETCH_ABSOLUTE
      case FETCH_RELATIVE => TFetchOrientation.FETCH_RELATIVE
      case FETCH_PRIOR => TFetchOrientation.FETCH_PRIOR
      case _ => TFetchOrientation.FETCH_NEXT
    }
  }
}
