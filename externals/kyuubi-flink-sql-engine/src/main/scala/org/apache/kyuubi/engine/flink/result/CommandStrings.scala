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

package org.apache.kyuubi.engine.flink.result

import scala.collection.mutable.ListBuffer

import org.apache.flink.util.Preconditions
import org.jline.utils.{AttributedString, AttributedStringBuilder, AttributedStyle}

/**
 * Utility class that contains all strings for Flink SQL commands and messages.
 */
object CommandStrings {
  private val CMD_DESC_DELIMITER = "\t\t"

  private class SQLCommandsDescriptions {
    private var commandMaxLength = -1
    private val commandsDescriptionList = ListBuffer[(String, String)]()

    def commandDescription(command: String, description: String): SQLCommandsDescriptions = {
      Preconditions.checkState(
        command.nonEmpty,
        s"content of command must not be empty.",
        Seq(): _*)
      Preconditions.checkState(
        description.nonEmpty,
        s"content of command's description must not be empty.",
        Seq(): _*)

      updateMaxCommandLength(command.length)
      commandsDescriptionList += ((command, description))
      this
    }

    private def updateMaxCommandLength(newLength: Int): Unit = {
      Preconditions.checkState(newLength > 0)
      if (commandMaxLength < newLength) {
        commandMaxLength = newLength
      }
    }

    private def formatDescription(input: String): String = {
      val maxLineLength = 160
      val newLinePrefix = " " * commandMaxLength + CMD_DESC_DELIMITER
      val words = input.split(" ")

      val (lastLine, lines) = words.foldLeft(("", List[String]())) {
        case ((line, lines), word) =>
          val newLine = if (line.isEmpty) word else line + " " + word
          if (newLine.length > maxLineLength) (word, lines :+ line) else (newLine, lines)
      }

      (lines :+ lastLine).mkString("\n" + newLinePrefix)
    }

    def build(): AttributedString = {
      val attributedStringBuilder = new AttributedStringBuilder
      if (commandsDescriptionList.nonEmpty) {
        commandsDescriptionList.foreach {
          case (cmd, cmdDesc) =>
            attributedStringBuilder
              .style(AttributedStyle.DEFAULT.bold())
              .append(cmd.padTo(commandMaxLength, " ").mkString)
              .append(CMD_DESC_DELIMITER)
              .style(AttributedStyle.DEFAULT)
              .append(formatDescription(cmdDesc))
              .append('\n')
        }
      }
      attributedStringBuilder.toAttributedString
    }
  }

  // scalastyle:off line.size.limit
  val MESSAGE_HELP: AttributedString =
    new AttributedStringBuilder()
      .append("The following commands are available:\n\n")
      .append(COMMANDS_DESCRIPTIONS)
      .style(AttributedStyle.DEFAULT.underline())
      .append("\nHint")
      .style(AttributedStyle.DEFAULT)
      .append(
        ": Make sure that a statement ends with \";\" for finalizing (multi-line) statements.")
      // About Documentation Link.
      .style(AttributedStyle.DEFAULT)
      .append(
        "\nThe above list includes only the most frequently used statements.\nYou can also type any Flink SQL statement, please visit https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/ for more details.")
      .toAttributedString

  def COMMANDS_DESCRIPTIONS: AttributedString =
    new SQLCommandsDescriptions()
      .commandDescription(
        "HELP",
        "Prints the available commands or the detailed description of a specified command.")
      .commandDescription(
        "SET",
        "Sets a session configuration property. Syntax: \"SET '<key>'='<value>';\". Use \"SET;\" for listing all properties.")
      .commandDescription(
        "RESET",
        "Resets a session configuration property. Syntax: \"RESET '<key>';\". Use \"RESET;\" for reset all session properties.")
      .commandDescription(
        "INSERT INTO",
        "Inserts the results of a SQL SELECT query into a declared table sink.")
      .commandDescription(
        "INSERT OVERWRITE",
        "Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data.")
      .commandDescription(
        "SELECT",
        "Executes a SQL SELECT query on the Flink cluster.")
      .commandDescription(
        "EXPLAIN",
        "Describes the execution plan of a query or table with the given name.")
      .commandDescription(
        "BEGIN STATEMENT SET",
        "Begins a statement set. Syntax: \"BEGIN STATEMENT SET;\"")
      .commandDescription("END", "Ends a statement set. Syntax: \"END;\"")
      .commandDescription(
        "ADD JAR",
        "Adds the specified jar file to the submitted jobs' classloader. Syntax: \"ADD JAR '<path_to_filename>.jar'\"")
      .commandDescription(
        "SHOW JARS",
        "Shows the list of user-specified jar dependencies. This list is impacted by the ADD JAR commands.")
      .commandDescription(
        "SHOW CATALOGS",
        "Shows the list of registered catalogs.")
      .commandDescription(
        "SHOW CURRENT CATALOG",
        "Shows the name of the current catalog.")
      .commandDescription(
        "SHOW DATABASES",
        "Shows all databases in the current catalog.")
      .commandDescription(
        "SHOW CURRENT DATABASE",
        "Shows the name of the current database.")
      .commandDescription(
        "SHOW TABLES",
        "Shows all tables for an optionally specified database. Syntax: \"SHOW TABLES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] LIKE <sql_like_pattern> ]\"")
      .commandDescription(
        "SHOW CREATE TABLE",
        "Shows the CREATE TABLE statement that creates the specified table.")
      .commandDescription(
        "SHOW COLUMNS",
        "Shows all columns of a table with the given name. Syntax: \"SHOW COLUMNS ( FROM | IN ) [[catalog_name.]database.]<table_name> [ [NOT] LIKE <sql_like_pattern>]\"")
      .commandDescription(
        "SHOW VIEWS",
        "Shows all views in the current catalog and the current database.")
      .commandDescription(
        "SHOW CREATE VIEW",
        "Shows the CREATE VIEW statement that creates the specified view. Syntax: \"SHOW CREATE VIEW [catalog_name.][db_name.]view_name\"")
      .commandDescription(
        "SHOW FUNCTIONS",
        "Shows all user-defined and built-in functions in the current catalog and current database. Use \"SHOW USER FUNCTIONS\" for listing all user-defined functions in the current catalog and current database.")
      .commandDescription(
        "SHOW MODULES",
        "Shows all enabled module names with resolution order.")
      .commandDescription(
        "USE CATALOG",
        "Sets the current catalog. All subsequent commands that do not explicitly specify a catalog will use this one. If the provided catalog does not exist, an exception is thrown. The default current catalog is default_catalog. Syntax: \"USE CATALOG catalog_name\"")
      .commandDescription(
        "USE",
        "Sets the current database. All subsequent commands that do not explicitly specify a database will use this one. If the provided database does not exist, an exception is thrown. The default current database is default_database. Syntax: \"USE [catalog_name.]database_name\"")
      .commandDescription(
        "DESC",
        "Describes the schema of a table with the given name. Syntax: \"{ DESCRIBE | DESC } [catalog_name.][db_name.]table_name\"")
      .commandDescription(
        "ANALYZE",
        "ANALYZE statements are used to collect statistics for existing tables and store the result to catalog. Only supports in batch mode. Syntax: \"ANALYZE TABLE [catalog_name.][db_name.]table_name PARTITION(partcol1[=val1] [, partcol2[=val2], ...]) COMPUTE STATISTICS [FOR COLUMNS col1 [, col2, ...] | FOR ALL COLUMNS]\"")
      .commandDescription(
        "ALTER TABLE",
        "Renames a table or change a table's properties. Syntax: \"ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name\", the other syntax: \"ALTER TABLE [catalog_name.][db_name.]table_name SET ( key1=val1[, key2=val2, ...] )\"")
      .commandDescription(
        "ALTER VIEW",
        "Renames a given view to a new name within the same catalog and database. Syntax: \"ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name\"")
      .commandDescription(
        "ALTER DATABASE",
        "Changes a database's properties. Syntax: \"ALTER DATABASE [catalog_name.]db_name SET ( key1=val1[, key2=val2, ...] )\"")
      .commandDescription(
        "ALTER FUNCTION",
        "Changes a catalog function with the new identifier and optional language tag. Syntax: \"ALTER [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name AS identifier [LANGUAGE JAVA|SCALA|PYTHON]\"")
      .commandDescription(
        "DROP CATALOG",
        "Drops a catalog with the given catalog name. Syntax: \"DROP CATALOG [IF EXISTS] catalog_name\"")
      .commandDescription(
        "DROP DATABASE",
        "Drops a database with the given database name. Syntax: \"DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]\"")
      .commandDescription(
        "DROP TABLE",
        "Drops a table with the given table name. Syntax: \"DROP [TEMPORARY] TABLE [IF EXISTS] [catalog_name.][db_name.]table_name\"")
      .commandDescription(
        "DROP VIEW",
        "Drops a view with the given view name. Syntax: \"DROP [TEMPORARY] VIEW  [IF EXISTS] [catalog_name.][db_name.]view_name\"")
      .commandDescription(
        "DROP FUNCTION",
        "Drops a catalog function with the given function name. Syntax: \"DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name\"")
      .commandDescription(
        "CREATE CATALOG",
        "Creates a catalog with the given catalog properties. Syntax: \"CREATE CATALOG catalog_name WITH ( 'key1'='value1'[, 'key2'='value2', ...] )\"")
      .commandDescription(
        "CREATE DATABASE",
        "Creates a database with the given database properties. Syntax: \"CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name [COMMENT 'database_comment'] [WITH ( 'key1'='value1'[, 'key2'='value2', ...] )]\"")
      .commandDescription(
        "CREATE TABLE",
        "Creates a table with the given table properties. Syntax: \"CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name ( { col_name data_type [COMMENT col_comment] [column_constraint] | table_constraint } [,...] ) [COMMENT table_comment] [PARTITIONED BY (col_name, col_name, ...)] [WITH ( 'key1'='value1'[, 'key2'='value2', ...] )] \"")
      .commandDescription(
        "CREATE VIEW",
        "Creates a view with the given view expression. Syntax: \"CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [catalog_name.][db_name.]view_name [(column_name [,...])] [COMMENT view_comment] AS query_expression\"")
      .commandDescription(
        "CREATE FUNCTION",
        "Creates a catalog function with the given function properties. Syntax: \"CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF NOT EXISTS] [catalog_name.][db_name.]function_name AS identifier [LANGUAGE JAVA|SCALA|PYTHON] [USING JAR '<path_to_filename>.jar' [, JAR '<path_to_filename>.jar']* ]\"")
      .commandDescription(
        "SHOW JOBS",
        "Show the jobs in the Flink cluster. Supports in version 1.17 and later.")
      .commandDescription(
        "STOP JOB",
        "Stop the job with the given job ID. Supports in version 1.17 and later. Syntax: \"STOP JOB '<job_id>' [WITH SAVEPOINT] [WITH DRAIN]\"")
      .commandDescription(
        "UPDATE",
        "Performs row-level updating on the target table. Only supports in batch mode. Supports in version 1.17 and later. Syntax: \"UPDATE [catalog_name.][db_name.]table_name SET col_name1 = col_val1 [, col_name2 = col_val2 ...] [WHERE condition]\"")
      .commandDescription(
        "DELETE",
        "Performs row-level deleting on the target table. Only supports in batch mode. Supports in version 1.17 and later. Syntax: \"DELETE FROM [catalog_name.][db_name.]table_name [WHERE condition]\"")
      .commandDescription(
        "TRUNCATE TABLE",
        "Truncates the target table. Only supports in batch mode. Supports in version 1.18 and later. Syntax: \"TRUNCATE TABLE [catalog_name.][db_name.]table_name\"")
      .commandDescription(
        "CALL",
        "Calls a stored procedure. Supports in version 1.18 and later. Syntax: \"CALL [catalog_name.][database_name.]procedure_name ([ expression [, expression]* ] )\"")
      .build()
  // scalastyle:on
}
