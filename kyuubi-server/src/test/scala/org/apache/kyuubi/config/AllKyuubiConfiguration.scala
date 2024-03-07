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

package org.apache.kyuubi.config

import java.nio.file.Paths

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiFunSuite, MarkdownBuilder}
import org.apache.kyuubi.ctl.CtlConf
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.metrics.MetricsConf
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf
import org.apache.kyuubi.util.GoldenFileUtils._
import org.apache.kyuubi.util.JavaUtils
import org.apache.kyuubi.zookeeper.ZookeeperConf

/**
 * End-to-end test cases for configuration doc file
 * The golden result file is "docs/configuration/settings.md".
 *
 * To run the entire test suite:
 * {{{
 *   KYUUBI_UPDATE=0 dev/gen/gen_all_config_docs.sh
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   dev/gen/gen_all_config_docs.sh
 * }}}
 */
class AllKyuubiConfiguration extends KyuubiFunSuite {
  private val kyuubiHome: String =
    JavaUtils.getCodeSourceLocation(getClass).split("kyuubi-server")(0)
  private val markdown = Paths.get(kyuubiHome, "docs", "configuration", "settings.md")
    .toAbsolutePath

  private def loadConfigs = Array(
    KyuubiConf,
    CtlConf,
    HighAvailabilityConf,
    JDBCMetadataStoreConf,
    MetricsConf,
    ZookeeperConf)

  test("Check all kyuubi configs") {
    loadConfigs

    val builder = MarkdownBuilder(licenced = true, getClass.getName)

    builder ++=
      s"""
         |# Configurations
         |
         |Kyuubi provides several ways to configure the system and corresponding engines.
         |
         |## Environments
         |
         |""" +=
      """You can configure the environment variables in `$KYUUBI_HOME/conf/kyuubi-env.sh`,
         | e.g, `JAVA_HOME`, then this java runtime will be used both for Kyuubi server instance and
         | the applications it launches. You can also change the variable in the subprocess's env
         | configuration file, e.g.`$SPARK_HOME/conf/spark-env.sh` to use more specific ENV for
         | SQL engine applications. see `$KYUUBI_HOME/conf/kyuubi-env.sh.template` as an example.
         | """ +=
      """ For the environment variables that only needed to be transferred into engine
        | side, you can set it with a Kyuubi configuration item formatted
        | `kyuubi.engineEnv.VAR_NAME`. For example, with `kyuubi.engineEnv.SPARK_DRIVER_MEMORY=4g`,
        | the environment variable `SPARK_DRIVER_MEMORY` with value `4g` would be transferred into
        | engine side. With `kyuubi.engineEnv.SPARK_CONF_DIR=/apache/confs/spark/conf`, the
        | value of `SPARK_CONF_DIR` on the engine side is set to `/apache/confs/spark/conf`.
        | """ += "## Kyuubi Configurations" +=
      """ You can configure the Kyuubi properties in
         | `$KYUUBI_HOME/conf/kyuubi-defaults.conf`, see
         | `$KYUUBI_HOME/conf/kyuubi-defaults.conf.template` as an example.
         | """

    KyuubiConf.getConfigEntries().asScala
      .toStream
      .filterNot(_.internal)
      .groupBy(_.key.split("\\.")(1))
      .toSeq.sortBy(_._1).foreach { case (category, entries) =>
        builder ++=
          s"""### ${category.capitalize}
             | Key | Default | Meaning | Type | Since
             | --- | --- | --- | --- | ---
             |"""

        entries.sortBy(_.key).foreach { c =>
          val dft = c.defaultValStr.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(
            s"${c.key}",
            s"$dft",
            s"${c.doc}",
            s"${c.typ}",
            s"${c.version}").mkString("|")
        }
      }

    builder ++=
      """## Spark Configurations
        |### Via spark-defaults.conf
        |""" +=
      """ Setting them in `$SPARK_HOME/conf/spark-defaults.conf`
        | supplies with default values for SQL engine application. Available properties can be
        | found at Spark official online documentation for
        | [Spark Configurations](https://spark.apache.org/docs/latest/configuration.html)
        | """ +=
      "### Via kyuubi-defaults.conf" +=
      """ Setting them in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`
        | supplies with default values for SQL engine application too. These properties will
        | override all settings in `$SPARK_HOME/conf/spark-defaults.conf`""" +=
      "### Via JDBC Connection URL" +=
      """ Setting them in the JDBC Connection URL
        | supplies session-specific for each SQL engine. For example:""" ++=
      // scalastyle:off
      """```
        |jdbc:hive2://localhost:10009/default;#spark.sql.shuffle.partitions=2;spark.executor.memory=5g
        |```""" +=
      // scalastyle:on
      "" +=
      "- **Runtime SQL Configuration**" +=
      """  - For [Runtime SQL Configurations](
        |https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration), they
        | will take affect every time""" +=
      "- **Static SQL and Spark Core Configuration**" +=
      """  - For [Static SQL Configurations](
        |https://spark.apache.org/docs/latest/configuration.html#static-sql-configuration) and
        | other spark core configs, e.g. `spark.executor.memory`, they will take effect if there
        | is no existing SQL engine application. Otherwise, they will just be ignored
        | """ +=
      "### Via SET Syntax" +=
      """Please refer to the Spark official online documentation for
        | [SET Command](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-conf-mgmt-set.html)
        |"""

    builder ++=
      """## Flink Configurations
        |### Via flink-conf.yaml""" += """Setting them in `$FLINK_HOME/conf/flink-conf.yaml`
        |
        | supplies with default values for SQL engine application.
        | Available properties can be found at Flink official online documentation for
        | [Flink Configurations]
        |(https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/)""" +=
      "### Via kyuubi-defaults.conf" +=
      """Setting them in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`
        | supplies with default values for SQL engine application too.
        | You can use properties with the additional prefix `flink.` to override settings in
        | `$FLINK_HOME/conf/flink-conf.yaml`.""" ++=
      """
        |For example:
        |```
        |flink.parallelism.default 2
        |flink.taskmanager.memory.process.size 5g
        |```""" +=
      """The below options in `kyuubi-defaults.conf` will set `parallelism.default: 2`
        | and `taskmanager.memory.process.size: 5g` into flink configurations.""" +=
      "### Via JDBC Connection URL" ++=
      "Setting them in the JDBC Connection URL supplies session-specific for each SQL engine." +
        " For example:" ++=
      // scalastyle:off
      """```
        | jdbc:hive2://localhost:10009/default;#flink.parallelism.default=2;flink.taskmanager.memory.process.size=5g
        |```""" +=
      // scalastyle:on
      "### Via SET Statements" +=
      """Please refer to the Flink official online documentation for [SET Statements]
        |(https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/set/)"""

    builder ++=
      """## Trino Configurations
        |### Via config.properties""" += """Setting them in `$TRINO_HOME/etc/config.properties`
        |
        | supplies with default values for SQL engine application.
        | Available properties can be found at Trino official online documentation for
        | [Trino Configurations]
        |(https://trino.io/docs/current/admin/properties.html)""" +=
      "### Via kyuubi-defaults.conf" +=
      """Setting them in `$KYUUBI_HOME/conf/kyuubi-defaults.conf`
        | supplies with default values for SQL engine application too.
        | You can use properties with the additional prefix `trino.` to override settings in
        | `$TRINO_HOME/etc/config.properties`.""" ++=
      """
        |For example:
        |```
        |trino.query_max_stage_count 500
        |trino.parse_decimal_literals_as_double true
        |```""" +=
      """The below options in `kyuubi-defaults.conf` will set `query_max_stage_count: 500`
        | and `parse_decimal_literals_as_double: true` into trino session properties.""" +=
      "### Via JDBC Connection URL" +=
      "Setting them in the JDBC Connection URL supplies session-specific for each SQL engine." +
        " For example:" ++=
      // scalastyle:off
      """ ```
        | jdbc:hive2://localhost:10009/default;#trino.query_max_stage_count=500;trino.parse_decimal_literals_as_double=true
        | ```
        |""" +=
      // scalastyle:on
      "### Via SET Statements" +=
      """Please refer to the Trino official online documentation for [SET Statements]
        |(https://trino.io/docs/current/sql/set-session.html)"""

    builder += "## Logging" +=
      """Kyuubi uses [log4j](https://logging.apache.org/log4j/2.x/) for logging.
        | You can configure it using `$KYUUBI_HOME/conf/log4j2.xml`, see
        | `$KYUUBI_HOME/conf/log4j2.xml.template` as an example.
        | """

    builder ++=
      """## Other Configurations
        |### Hadoop Configurations
        |""" +=
      """Specifying `HADOOP_CONF_DIR` to the directory containing Hadoop configuration
        | files or treating them as Spark properties with a `spark.hadoop.` prefix.
        | Please refer to the Spark official online documentation for
        | [Inheriting Hadoop Cluster Configuration](https://spark.apache.org/docs/latest/
        |configuration.html#inheriting-hadoop-cluster-configuration).
        | Also, please refer to the [Apache Hadoop](https://hadoop.apache.org)'s
        | online documentation for an overview on how to configure Hadoop.""" +=
      "### Hive Configurations" +=
      """These configurations are used for SQL engine application to talk to
        | Hive MetaStore and could be configured in a `hive-site.xml`.
        | Placed it in `$SPARK_HOME/conf` directory, or treat them as Spark properties with
        | a `spark.hadoop.` prefix."""

    builder += "## User Defaults" +=
      """In Kyuubi, we can configure user default settings to meet separate needs.
        | These user defaults override system defaults, but will be overridden by those from
        | [JDBC Connection URL](#via-jdbc-connection-url) or [Set Command](#via-set-syntax)
        | if could be. They will take effect when creating the SQL engine application ONLY.""" +=
      """User default settings are in the form of `___{username}___.{config key}`.
        | There are three continuous underscores(`_`) at both sides of the `username` and
        | a dot(`.`) that separates the config key and the prefix. For example:""" ++=
      """```bash
        |# For system defaults
        |spark.master=local
        |spark.sql.adaptive.enabled=true
        |# For a user named kent
        |___kent___.spark.master=yarn
        |___kent___.spark.sql.adaptive.enabled=false
        |# For a user named bob
        |___bob___.spark.master=spark://master:7077
        |___bob___.spark.executor.memory=8g
        |```
        |
        |""" +=
      """In the above case, if there are related configurations from
        | [JDBC Connection URL](#via-jdbc-connection-url), `kent` will run his SQL engine
        | application on YARN and prefer the Spark AQE to be off, while `bob` will activate
        | his SQL engine application on a Spark standalone cluster with 8g heap memory for each
        | executor and obey the Spark AQE behavior of Kyuubi system default. On the other hand,
        | for those users who do not have custom configurations will use system defaults."""

    verifyOrRegenerateGoldenFile(markdown, builder.toMarkdown, "dev/gen/gen_all_config_docs.sh")
  }
}
