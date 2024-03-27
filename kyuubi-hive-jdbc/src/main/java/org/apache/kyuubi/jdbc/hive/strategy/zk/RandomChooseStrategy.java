package org.apache.kyuubi.jdbc.hive.strategy.zk;

import org.apache.kyuubi.jdbc.hive.strategy.ChooseServerStrategy;
import org.apache.kyuubi.shaded.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomChooseStrategy implements ChooseServerStrategy {
    @Override
    public String chooseServer(List<String> serverHosts, CuratorFramework zkClient, String namespace) {
        return serverHosts.get(ThreadLocalRandom.current().nextInt(serverHosts.size()));
    }
}
