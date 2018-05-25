# Authentication/Security Guide
Kyuubi supports Anonymous (no authentication) with and without SASL, Kerberos (GSSAPI), pass through LDAP between the Thrift client and itself.

## Configuration

Name|Default|Description
---|---|---
spark.kyuubi.authentication | NONE | Authentication mode, default NONE. Options are NONE (uses plain SASL), NOSASL, KERBEROS, LDAP.

#### NONE
###### Server
```bash
$KYUUBI_HOME/bin/start-kyuubi.sh --conf spark.kyuubi.authentication=NONE
```
###### Client
```bash
$SPARK_HOME/bin/beeline -u "jdbc:hive2://${replace with spark.kyuubi.frontend.bind.host}:10009/;hive.server2.proxy.user=yaooqinn"
```

#### NOSASL
###### Server
```bash
$KYUUBI_HOME/bin/start-kyuubi.sh --conf spark.kyuubi.authentication=NOSASL
```
###### Client
```bash
$SPARK_HOME/bin/beeline -u "jdbc:hive2://${replace with spark.kyuubi.frontend.bind.host}:10009/;hive.server2.proxy.user=hzyaoqin;auth=noSasl"
```


#### KERBEROS

If you configure Kyuubi to use Kerberos authentication, Kyuubi acquires a Kerberos ticket during startup. Kyuubi requires a principal and keytab file specified in `$SPARK_HOME/conf/spark-defaults.conf`. Client applications (for example, JDBC or Beeline) must have a valid Kerberos ticket before initiating a connection to Kyuubi.

Set following for KERBEROS mode:
- spark.yarn.principal – Kerberos principal for Kyuubi server.
- spark.yarn.keytab – Keytab for Kyuubi server principal.

**NOTE:**: NONE and NOSASL mode also support these two configurations for Kyuubi to talk with a kerberized cluster only without verifying client accessing via kerberos.
###### Server
```bash
$KYUUBI_HOME/bin/start-kyuubi.sh --conf spark.kyuubi.authentication=KERBEROS
```
###### Client
```bash
$SPARK_HOME/bin/beeline -u "jdbc:hive2://${replace with spark.kyuubi.frontend.bind.host}:10000/;principal=${replace with spark.yarn.principal};hive.server2.proxy.user=yaooqinn"
```

[Home Page](https://yaooqinn.github.io/kyuubi/)