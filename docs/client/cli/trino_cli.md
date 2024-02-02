<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Trino command line interface

The Trino CLI provides a terminal-based, interactive shell for running queries. We can use it to connect Kyuubi server now.

## Start Kyuubi Trino Server

First we should configure the trino protocol and the service port in the `kyuubi-defaults.conf`

```
kyuubi.frontend.protocols TRINO
kyuubi.frontend.trino.bind.port 10999 #default port
```

## Install

Download [trino-cli-411-executable.jar](https://repo1.maven.org/maven2/io/trino/trino-jdbc/411/trino-jdbc-411.jar), rename it to `trino`, make it executable with `chmod +x`, and run it to show the version of the CLI:

```
wget https://repo1.maven.org/maven2/io/trino/trino-jdbc/411/trino-jdbc-411.jar
mv trino-jdbc-411.jar trino
chmod +x trino
./trino --version
```

## Running the CLI

The minimal command to start the CLI in interactive mode specifies the URL of the kyuubi server with the Trino protocol:

```
./trino --server http://localhost:10999
```

If successful, you will get a prompt to execute commands. Use the help command to see a list of supported commands. Use the clear command to clear the terminal. To stop and exit the CLI, run exit or quit.:

```
trino> help

Supported commands:
QUIT
EXIT
CLEAR
EXPLAIN [ ( option [, ...] ) ] <query>
    options: FORMAT { TEXT | GRAPHVIZ | JSON }
             TYPE { LOGICAL | DISTRIBUTED | VALIDATE | IO }
DESCRIBE <table>
SHOW COLUMNS FROM <table>
SHOW FUNCTIONS
SHOW CATALOGS [LIKE <pattern>]
SHOW SCHEMAS [FROM <catalog>] [LIKE <pattern>]
SHOW TABLES [FROM <schema>] [LIKE <pattern>]
USE [<catalog>.]<schema>
```

You can now run SQL statements. After processing, the CLI will show results and statistics.

```
trino> select 1;
 _col0
-------
     1
(1 row)

Query 20230216_125233_00806_examine_6hxus, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.29 [0 rows, 0B] [0 rows/s, 0B/s]

trino>
```

Many other options are available to further configure the CLI in interactive mode to
refer https://trino.io/docs/current/client/cli.html#running-the-cli
