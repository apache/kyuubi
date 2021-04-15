# Kyuubi High Availability Guide

As an enterprise-class ad-hoc SQL query service built on top of [Apache Spark](http://spark.apache.org/), Kyuubi takes high availability(HA) as a major characteristic, aiming to ensure an agreed level of service availability, such as a higher than normal period of uptime.

Running Kyuubi in HA mode is to use groups of computers or containers that support SQL query service on Kyuubi that can be reliably utilized with a minimum amount of down-time. Kyuubi operates by using [Apache ZooKeeper](https://zookeeper.apache.org/) to harness redundant service instances in groups that provide continuous service when one or more components fail.

Without HA, if a server crashes, Kyuubi will be unavailable until the crashed server is fixed. With HA, this situation will be remedied by hardware/software faults auto detecting, and immediately another Kyuubi service instance will be ready to serve without requiring human intervention. 

## High Availability Mode Types

Kyuubi supports two different types of HA mode. One is load balance mode, and the other active/standby failover.

Load balance mode means that all Kyuubi server instances are active at the first place and their service uri can be reached by clients through the Zookeeper. This mode can greatly reduce the load on the instance itself, on the contrary the load on the cluster manager(YARN) may go higher for there may be more than one Spark application running for a single user who is connected. Another thing you need to know is that applying resources from YARN to launch an application is time consuming.

Active/Standby failover is another option for you to make Kyuubi system highly available. Only one node is the primary node and visible for clients via ZooKeeper and all the others are secondary ones during the service period and invisble. In this mode, the standby will become serviceable only when it has gained the leadership then publish its service uri to ZooKeeper, meanwhile the previous active one's uri will be retired. There will be only one Spark application running on YARN for a single user connected. YARN's load will not be as heavy as in load balance mode and the overhead for launching applications will be certainly reduced. On the other side, the active server will withstand more traffic. Anyway, it is a good choice for those small Hadoop clusters to apply this mode to gain high availability.

The number of user concurrency and the size of your cluster may be two major indicators which you need to weigh against the online environment.

## Load Balance Mode

Load balancing aims to optimize all Kyuubi service units usage, maximize throughput, minimize response time, and avoid overload of a single unit. Using multiple Kyuubi service units with load balancing instead of a single unit may increase reliability and availability through redundancy. 

![](../imgs/ha.png)

With Hive JDBC Driver, a client can specify service discovery mode in JDBC connection string, i.e. `serviceDiscoveryMode=zooKeeper;` and set `zooKeeperNameSpace=kyuubiserver;`, then it can randomly pick one of the Kyuubi service uris from the specified ZooKeeper address in the `/kyuubiserver` path.

When we set `kyuubi.ha.enabled` to `true`, load balance mode is activated by default. Please make sure that you specify the correct ZooKeeper address via `kyuubi.ha.zookeeper.quorum` and `kyuubi.ha.zookeeper.client.port`.

## Active/Standby Failover

Active/Standby failover enables you to use a standby Kyuubi server to take over the functionality of a failed unit. When the active unit fails, it changes to the standby state after fixed while the standby unit changes to the active state.

![](../imgs/ha_failover.png)

A client need not to change any of its behaviours to support load balance or failover mode. But because only the active Kyuubi server will expose its service uri to ZooKeeper in `/kyuubiserver`, clients always randomly pick a server from one and the only choice.

When we set `kyuubi.ha.enabled` to `true` and `kyuubi.ha.mode` to `failover`, failover mode is activated then. Please make sure that you specify the correct ZooKeeper address via `kyuubi.ha.zookeeper.quorum` and `kyuubi.ha.zookeeper.client.port`.

## Configuring High Availability

This section describes how to configure high availability. These configurations in the following table can be treat like normal Spark properties by setting them in `spark-defaults.conf` file or via `--conf` parameter in server starting scripts.

Key | Default | Meaning | Since
--- | --- | --- | ---
kyuubi\.ha\.zookeeper<br>\.acl\.enabled|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>false</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Set to true if the zookeeper ensemble is kerberized</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.connection\.base\.retry<br>\.wait|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>1000</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Initial amount of time to wait between retries to the zookeeper ensemble</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.connection\.max<br>\.retries|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>3</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Max retry times for connecting to the zookeeper ensemble</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.connection\.max\.retry<br>\.wait|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>30000</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>Max amount of time to wait between retries for BOUNDED_EXPONENTIAL_BACKOFF policy can reach, or max time until elapsed for UNTIL_ELAPSED policy to connect the zookeeper ensemble</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.connection\.retry<br>\.policy|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>EXPONENTIAL_BACKOFF</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>The retry policy for connecting to the zookeeper ensemble, all candidates are: <ul><li>ONE_TIME</li><li> N_TIME</li><li> EXPONENTIAL_BACKOFF</li><li> BOUNDED_EXPONENTIAL_BACKOFF</li><li> UNTIL_ELAPSED</li></ul></div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.connection\.timeout|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>15000</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>The timeout(ms) of creating the connection to the zookeeper ensemble</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.namespace|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>kyuubi</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>The root directory for the service to deploy its instance uri. Additionally, it will creates a -[username] suffixed root directory for each application</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.quorum|<div style='width: 80pt;word-wrap: break-word;white-space: normal'></div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>The connection string for the zookeeper ensemble</div>|<div style='width: 20pt'>1.0.0</div>
kyuubi\.ha\.zookeeper<br>\.session\.timeout|<div style='width: 80pt;word-wrap: break-word;white-space: normal'>60000</div>|<div style='width: 200pt;word-wrap: break-word;white-space: normal'>The timeout(ms) of a connected session to be idled</div>|<div style='width: 20pt'>1.0.0</div>

## Additional Documentations
[Building Kyuubi](https://NetEase.github.io/kyuubi/docs/building.html)  
[Kyuubi Deployment Guide](https://NetEase.github.io/kyuubi/docs/deploy.html)  
[Kyuubi Containerization Guide](https://NetEase.github.io/kyuubi/docs/containerization.html)   
[Configuration Guide](https://NetEase.github.io/kyuubi/docs/configurations.html)  
[Authentication/Security Guide](https://NetEase.github.io/kyuubi/docs/authentication.html)  
[Kyuubi ACL Management Guide](https://NetEase.github.io/kyuubi/docs/authorization.html)  
[Kyuubi Architecture](https://NetEase.github.io/kyuubi/docs/architecture.html)
