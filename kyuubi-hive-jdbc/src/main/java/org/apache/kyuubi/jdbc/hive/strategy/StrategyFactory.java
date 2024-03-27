package org.apache.kyuubi.jdbc.hive.strategy;

import org.apache.kyuubi.jdbc.hive.ZooKeeperHiveClientException;
import org.apache.kyuubi.jdbc.hive.strategy.zk.PollingChooseStrategy;
import org.apache.kyuubi.jdbc.hive.strategy.zk.RandomChooseStrategy;

import java.lang.reflect.Constructor;

public class StrategyFactory {
    public static ChooseServerStrategy createStrategy(String strategy) throws ZooKeeperHiveClientException {
        try {
            switch (strategy) {
                case "poll":
                    return new PollingChooseStrategy();
                case "random":
                    return new RandomChooseStrategy();
                default:
                    Class<?> clazz = Class.forName(strategy);
                    if (ChooseServerStrategy.class.isAssignableFrom(clazz)) {
                        Constructor<? extends ChooseServerStrategy> constructor = clazz.asSubclass(ChooseServerStrategy.class).getConstructor();
                        return constructor.newInstance();
                    } else {
                        throw new ZooKeeperHiveClientException("The loaded class does not implement ChooseServerStrategy");
                    }
            }
        }catch (Exception e){
            throw new ZooKeeperHiveClientException("Oops, load the chooseStrategy is wrong, please check your connection params");
        }
    }
}
