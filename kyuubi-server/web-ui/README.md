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

# Apache Kyuubi(incubating) Web UI

The web ui listens at `http://localhost:10099`.

The web ui is work in progress. Kyuubi server starts an HTTP server (by default at port 10009)
that serves the new web pages and additional background requests.

## Server Backend

The server side of the ui is implemented using [Jetty](https://www.eclipse.org/jetty/) 
for REST paths.
The framework has very lightweight dependencies.

## UI Frontend 

The web ui is implemented using *Angular*. The ui build infrastructure uses *Node.js*.

### Preparing the Build Environment

Depending on your version of Linux, Windows or macOS, you may need to manually install *node.js*

#### Ubuntu Linux

Install *node.js* by following [these instructions](https://nodejs.org/en/download/).

Verify that the installed version is at least *16.x.x*, via `node --version`.

#### MacOS

First install *brew* by following [these instructions](http://brew.sh/).

Install *node.js* via:

```
brew install node@16
```

### Building

The build process downloads all requires libraries via the *node.js* package management tool (*npm*)
The final build tool is *@angular/cli*.

```
cd kyuubi-server/web-ui
npm install
npm run build
```

The ui code is under `/src`. The result of the build process is under `/web`.

### Developing

When developing the ui, every change needs to recompile the files and update the server:

```
cd kyuubi-server/web-ui
npm run build
cd ../..
mvn -DskipTests clean package
```

To simplify continuous development, one can use a *standalone proxy server*, together with automatic
re-compilation:

1. Start the proxy server via `npm run proxy` (You can modify the proxy target in the `proxy.conf.json`, the default proxy target is `localhost:10009`)
2. Access the ui at [`http://localhost:4200`](http://localhost:4200)

### CodeStyle & Lint

```bash
$ npm run lint
```

### Dependency

- Framework: [Angular](https://angular.io)
- CLI Tools: [Angular CLI](https://cli.angular.io)
- UI Components: [NG-ZORRO](https://github.com/NG-ZORRO/ng-zorro-antd)
