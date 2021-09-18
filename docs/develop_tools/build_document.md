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

# Building Kyuubi Documentation

Follow the steps below and learn how to build the Kyuubi documentation as the one you are watching now.

## Install & Activate `virtualenv`

Firstly, install `virtualenv`, this is optional but recommended as it is useful to create an independent environment to resolve dependency issues for building the documentation.

```bash
pip install virtualenv
```

Switch to the `docs` root directory.

```bash
cd $KTUUBI_HOME/docs
```

Create a virtual environment named 'kyuubi' or anything you like using `virtualenv` if it's not existing.

```bash
virtualenv kyuubi
```

Activate it,

```bash
 source ./kyuubi/bin/activate
```

## Install all dependencies

Install all dependencies enumerated in the `requirements.txt`

```bash
pip install -r requirements.txt
```

## Create Documentation

```bash
make html
```

If the build process succeed, the HTML pages are in `_build/html`.

## View Locally

Open the `_build/html/index.html` file in your favorite web browser.
