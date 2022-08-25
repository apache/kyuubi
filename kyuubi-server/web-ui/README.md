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
# Kyuubi Web UI

### Start Using

For the best experience, we recommend using `node 16.x.x`.
You can learn how to install the corresponding version from its official website.

- [node](https://nodejs.org/en/)

### Install Dependencies

```shell
npm install
```

### Development Project

To do this you can change the VITE_APP_DEV_WEB_URL parameter variable as the service url in `.env.development` in the project root directory, such as http://127.0. 0.1:8090


```shell
npm run dev
```

### Build Project

```shell
npm run build
```

### Code Format

Usually after you modify the code, you need to perform code formatting operations to ensure that the code in the project is the same style.

```shell
npm run prettier
```

### Recommend

If you want to save disk space and boost installation speed, we recommend using `pnpm 7.x.x` to instead of npm.
You can learn how to install the corresponding version from its official website.

- [pnpm](https://pnpm.io/)

```shell
# Install Dependencies
pnpm install

# Development Project
pnpm run dev

# Build Project
pnpm run build

# Code Format
pnpm run prettier
```
