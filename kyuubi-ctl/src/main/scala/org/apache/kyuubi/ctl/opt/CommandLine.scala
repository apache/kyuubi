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

package org.apache.kyuubi.ctl.opt

import scopt.{OParser, OParserBuilder}

import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.ctl.util.DateTimeUtils.dateStringToMillis

object CommandLine extends CommonCommandLine {

  def getCtlOptionParser(builder: OParserBuilder[CliConfig]): OParser[Unit, CliConfig] = {
    import builder._
    OParser.sequence(
      programName("kyuubi-ctl"),
      head("kyuubi", KYUUBI_VERSION),
      common(builder),
      zooKeeper(builder),
      create(builder),
      get(builder),
      delete(builder),
      list(builder),
      log(builder),
      submit(builder),
      checkConfig(f => {
        if (f.action == null) {
          failure("Must specify action command: [create|get|delete|list|log|submit].")
        } else {
          success
        }
      }),
      note(""),
      help('h', "help").text("Show help message and exit."))
  }

  private def zooKeeper(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      opt[String]("zk-quorum").abbr("zk")
        .action((v, c) => c.copy(zkOpts = c.zkOpts.copy(zkQuorum = v)))
        .text("The connection string for the zookeeper ensemble," +
          " using zk quorum manually."),
      opt[String]('n', "namespace")
        .action((v, c) => c.copy(zkOpts = c.zkOpts.copy(namespace = v)))
        .text("The namespace, using kyuubi-defaults/conf if absent."),
      opt[String]('s', "host")
        .action((v, c) => c.copy(zkOpts = c.zkOpts.copy(host = v)))
        .text("Hostname or IP address of a service."),
      opt[String]('p', "port")
        .action((v, c) => c.copy(zkOpts = c.zkOpts.copy(port = v)))
        .text("Listening port of a service."),
      opt[String]('v', "version")
        .action((v, c) => c.copy(zkOpts = c.zkOpts.copy(version = v)))
        .text("Using the compiled KYUUBI_VERSION default," +
          " change it if the active service is running in another."))
  }

  private def create(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("create")
        .text("\tCreate a resource.")
        .action((_, c) => c.copy(action = ControlAction.CREATE))
        .children(
          opt[String]('f', "filename")
            .action((v, c) => c.copy(createOpts = c.createOpts.copy(filename = v)))
            .text("Filename to use to create the resource"),
          createBatchCmd(builder).text("\tOpen batch session."),
          serverCmd(builder).text("\tExpose Kyuubi server instance to another domain.")))
  }

  private def get(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("get")
        .text("\tDisplay information about the specified resources.")
        .action((_, c) => c.copy(action = ControlAction.GET))
        .children(
          getBatchCmd(builder).text("\tGet batch by id."),
          serverCmd(builder).text("\tGet Kyuubi server info of domain"),
          engineCmd(builder).text("\tGet Kyuubi engine info belong to a user.")))

  }

  private def delete(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("delete")
        .text("\tDelete resources.")
        .action((_, c) => c.copy(action = ControlAction.DELETE))
        .children(
          deleteBatchCmd(builder).text("\tClose batch session."),
          serverCmd(builder).text("\tDelete the specified service node for a domain"),
          engineCmd(builder).text("\tDelete the specified engine node for user.")))

  }

  private def list(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("list")
        .text("\tList information about resources.")
        .action((_, c) => c.copy(action = ControlAction.LIST))
        .children(
          listBatchCmd(builder).text("\tList batch session info."),
          sessionCmd(builder).text("\tList all the live sessions"),
          serverCmd(builder).text("\tList all the service nodes for a particular domain"),
          engineCmd(builder).text("\tList all the engine nodes for a user")))

  }

  private def log(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("log")
        .text("\tPrint the logs for specified resource.")
        .action((_, c) => c.copy(action = ControlAction.LOG))
        .children(
          opt[Unit]("forward")
            .action((_, c) => c.copy(logOpts = c.logOpts.copy(forward = true)))
            .text("If forward is specified, the ctl will block forever."),
          logBatchCmd(builder).text("\tGet batch session local log.")))
  }

  private def submit(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("submit")
        .text("\tCombination of create, get and log commands.")
        .action((_, c) => c.copy(action = ControlAction.SUBMIT))
        .children(
          opt[String]('f', "filename")
            .action((v, c) => c.copy(createOpts = c.createOpts.copy(filename = v)))
            .text("Filename to use to create the resource"),
          submitBatchCmd(builder).text("\topen batch session and wait for completion.")))
  }

  private def sessionCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("session").action((_, c) => c.copy(resource = ControlObject.SESSION))
  }

  private def serverCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("server").action((_, c) => c.copy(resource = ControlObject.SERVER))
  }

  private def engineCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("engine").action((_, c) => c.copy(resource = ControlObject.ENGINE))
      .children(
        opt[String]('u', "user")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(user = v)))
          .text("The user name this engine belong to."),
        opt[String]("engine-type").abbr("et")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(engineType = v)))
          .text("The engine type this engine belong to."),
        opt[String]("engine-subdomain").abbr("es")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(engineSubdomain = v)))
          .text("The engine subdomain this engine belong to."),
        opt[String]("engine-share-level").abbr("esl")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(engineShareLevel = v)))
          .text("The engine share level this engine belong to."))
  }

  private def createBatchCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("batch").action((_, c) => c.copy(resource = ControlObject.BATCH))
  }

  private def getBatchCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("batch").action((_, c) => c.copy(resource = ControlObject.BATCH))
      .children(
        arg[String]("<batchId>")
          .optional()
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(batchId = v)))
          .text("Batch id."))
  }

  private def deleteBatchCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("batch").action((_, c) => c.copy(resource = ControlObject.BATCH))
      .children(
        arg[String]("<batchId>")
          .optional()
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(batchId = v)))
          .text("Batch id."))
  }

  private def listBatchCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("batch").action((_, c) => c.copy(resource = ControlObject.BATCH))
      .children(
        opt[String]("batchType")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(batchType = v)))
          .text("Batch type."),
        opt[String]("batchUser")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(batchUser = v)))
          .text("Batch user."),
        opt[String]("batchState")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(batchState = v)))
          .text("Batch state."),
        opt[String]("createTime")
          .action((v, c) =>
            c.copy(batchOpts = c.batchOpts.copy(createTime =
              dateStringToMillis(v, "yyyyMMddHHmmss"))))
          .validate(x =>
            if (x.matches("\\d{14}")) {
              success
            } else {
              failure("Option --createTime must be in yyyyMMddHHmmss format.")
            })
          .text("Batch create time, should be in yyyyMMddHHmmss format."),
        opt[String]("endTime")
          .action((v, c) =>
            c.copy(batchOpts = c.batchOpts.copy(endTime =
              dateStringToMillis(v, "yyyyMMddHHmmss"))))
          .validate(x =>
            if (x.matches("\\d{14}")) {
              success
            } else {
              failure("Option --endTime must be in yyyyMMddHHmmss format.")
            })
          .text("Batch end time, should be in yyyyMMddHHmmss format."),
        opt[Int]("from")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(from = v)))
          .validate(x =>
            if (x >= 0) {
              success
            } else {
              failure("Option --from must be >=0")
            })
          .text("Specify which record to start from retrieving info."),
        opt[Int]("size")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(size = v)))
          .validate(x =>
            if (x >= 0) {
              success
            } else {
              failure("Option --size must be >=0")
            })
          .text("The max number of records returned in the query."))
  }

  private def logBatchCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("batch").action((_, c) => c.copy(resource = ControlObject.BATCH))
      .children(
        arg[String]("<batchId>")
          .optional()
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(batchId = v)))
          .text("Batch id."),
        opt[Int]("from")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(from = v)))
          .text("Specify which record to start from retrieving info."),
        opt[Int]("size")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(size = v)))
          .validate(x =>
            if (x >= 0) {
              success
            } else {
              failure("Option --size must be >=0")
            })
          .text("The max number of records returned in the query."))
  }

  private def submitBatchCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("batch").action((_, c) => c.copy(resource = ControlObject.BATCH))
      .children(
        opt[Boolean]("waitCompletion")
          .action((v, c) => c.copy(batchOpts = c.batchOpts.copy(waitCompletion = v)))
          .text("Boolean property. If true(default), the client process will stay alive " +
            "until the batch is in any terminal state. If false, the client will exit " +
            "when the batch is no longer in PENDING state."))
  }

}
