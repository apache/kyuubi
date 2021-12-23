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

<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Developer Tools

## Update Project Version

```bash

build/mvn versions:set -DgenerateBackupPoms=false
```

## Update Document Version

Whenever project version updates, please also update the document version at `docs/conf.py` to target the upcoming release.

For example,

```python
release = '1.2.0'
```

## Update Dependency List

Kyuubi uses the `dev/dependencyList` file to indicate what upstream dependencies will actually go to the server-side classpath.

For Pull requests, a linter for dependency check will be automatically executed in GitHub Actions.


You can run `build/dependency.sh` locally first to detect the potential dependency change first.

If the changes look expected, run `build/dependency.sh --replace` to update `dev/dependencyList` in your Pull request.
