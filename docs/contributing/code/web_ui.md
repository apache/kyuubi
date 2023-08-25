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

# Web UI

Requirement:

1. Install [nodejs](https://nodejs.org/en/)[Recommend 18.x.x]

Recommend:

1. Using nodejs with version `18.x`
2. Using pnpm for package management with version `8.x`

## Run Web UI in Development Mode

Notice:

Before you start the Web UI project, please make sure the Kyuubi server has been started.

Kyuubi Web UI will proxy the requests to Kyuubi server, with the default endpoint path to`http://localhost:10099`.
Modify `VITE_APP_DEV_WEB_URL` in `.env.development` for customizing targeted endpoint path.

> Current Kyuubi server binds on `http://0.0.0.0:10099` in case you are running Kyuubi server in macOS or Windows(If in
> Linux, you should config Kyuubi server `kyuubi.frontend.rest.bind.host=0.0.0.0`, or change `VITE_APP_DEV_WEB_URL`
> in `.env.development`).

```shell
cd ${KYUUBI_HOME}/kyuubi-server/webui
# install dependencies
npm install
# run dev server
npm run dev
```

You can access Kyuubi Web UI at `http://localhost:9090/ui`

## Build Kyuubi Web UI

```shell
npm run build
```

## Code Style

Kyuubi Web UI using prettier for code style, you can run `npm run prettier` to format the code.

## Recommendation

If you want to save disk space and boost installation speed, we recommend using `pnpm 8.x.x` to instead of npm.
You can learn how to install the corresponding version from its official website.

```shell
# Install Dependencies
pnpm install

# Run Dev Server
pnpm run dev

# Build Project
pnpm run build

# Code Format
pnpm run prettier
```