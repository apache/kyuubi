package org.apache.kyuubi.cli

import org.apache.hive.service.rpc.thrift.TProtocolVersion

trait Handle {
  def identifier: HandleIdentifier
  def protocol: TProtocolVersion
}
