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

package yaooqinn.kyuubi.cli

import org.apache.hive.service.cli.thrift.TGetInfoType

trait GetInfoType {
  def tInfoType: TGetInfoType = null
}

case object DBMSName extends GetInfoType {
  override val tInfoType: TGetInfoType = TGetInfoType.CLI_DBMS_NAME
}

case object ServerName extends GetInfoType {
  override val tInfoType: TGetInfoType = TGetInfoType.CLI_SERVER_NAME
}

case object DBMSVesion extends  GetInfoType {
  override val tInfoType: TGetInfoType = TGetInfoType.CLI_DBMS_VER
}

object GetInfoType {
  def getGetInfoType(tGetInfoType: TGetInfoType): GetInfoType = tGetInfoType match {
    case DBMSName.tInfoType => DBMSName
    case ServerName.tInfoType => ServerName
    case DBMSVesion.tInfoType => DBMSVesion
    case _ =>
      throw new IllegalArgumentException("Unrecognized Thrift TGetInfoType value: " + tGetInfoType)
  }
}

