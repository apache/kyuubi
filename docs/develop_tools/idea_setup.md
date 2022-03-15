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

# IntelliJ IDEA Setup Guide

## Copyright Profile

Every file needs to include the Apache license as a header. This can be automated in IntelliJ by
adding a Copyright profile:

1. Go to "Settings" → "Editor" → "Copyright" → "Copyright Profiles".
2. Add a new profile and name it "Apache".
3. Add the following text as the license text:

   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
   ```
4. Go to "Editor" → "Copyright" and choose the "Apache" profile as the default profile for this
   project.
5. Click "Apply".

## Required Plugins

Go to "Settings" → "Plugins" and select the "Marketplace" tab. Search for the following plugins,
install them, and restart the IDE if prompted:

* [Scala](https://plugins.jetbrains.com/plugin/1347-scala)

## Formatter For Scala

Enable [Scalafmt](https://scalameta.org/scalafmt/) as follows:

1. Go to "Preferences" → "Editor" → "Code Style" → "Scala"
2. Set "Formatter" to "Scalafmt"
3. Enable "Reformat on file save"

## Checkstyle For Scala

Enable [Scalastyle](http://www.scalastyle.org/) as follows:

1. Go to "Settings" → "Editor" → "Inspections".
2. Search for "Scala style inspection" and enable it.

