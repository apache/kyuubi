package org.apache.kyuubi.engine.flink.security.token;

import org.apache.flink.runtime.security.token.hadoop.HadoopDelegationTokenReceiver;

public class KyuubiDelegationTokenReceiver extends HadoopDelegationTokenReceiver {

  @Override
  public String serviceName() {
    return "kyuubi";
  }
}
