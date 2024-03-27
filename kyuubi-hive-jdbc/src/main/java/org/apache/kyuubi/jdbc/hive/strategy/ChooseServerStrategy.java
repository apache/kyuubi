package org.apache.kyuubi.jdbc.hive.strategy;

import org.apache.kyuubi.jdbc.hive.ZooKeeperHiveClientException;
import org.apache.kyuubi.shaded.curator.framework.CuratorFramework;

import java.util.List;

public interface ChooseServerStrategy {
    String chooseServer(List<String> serverHosts, CuratorFramework zkClient, String namespace)throws ZooKeeperHiveClientException;
}
