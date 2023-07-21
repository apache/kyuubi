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

# IntelliJ IDEA Setup Guide

## Copyright Profile

Every file needs to include the Apache license as a header. This can be automated in IntelliJ by adding a Copyright
profile:

1. Go to "Settings/Preferences" → "Editor" → "Copyright" → "Copyright Profiles".
2. Add a new profile and name it "Apache".
3. Add the following text as the copyright text:

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
4. Go to "Editor" → "Copyright" and choose the "Apache" profile as the default profile for this project.
5. Click "Apply".

## Required Plugins

Go to "Settings/Preferences" → "Plugins" and select the "Marketplace" tab. Search for the following plugins, install
them, and restart the IDE if prompted:

* [Scala](https://plugins.jetbrains.com/plugin/1347-scala)

You will also need to install the [google-java-format](https://github.com/google/google-java-format)
plugin. However, a specific version of this plugin is required. Download
[google-java-format v1.7.0.6](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/115957)
and install it as follows. Make sure to NEVER update this plugin.

1. Go to "Settings/Preferences" → "Plugins".
2. Click the gear icon and select "Install Plugin from Disk".
3. Navigate to the downloaded ZIP file and select it.

## Formatter For Java

Kyuubi uses [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven) together with
[google-java-format](https://github.com/google/google-java-format) to format the Java code.

It is recommended to automatically format your code by applying the following settings:

1. Go to "Settings/Preferences" → "Other Settings" → "google-java-format Settings".
2. Tick the checkbox to enable the plugin.
3. Change the code style to "Default Google Java style".
4. Go to "Settings/Preferences" → "Tools" → "Actions on Save".
5. Select "Reformat code".

If you use the IDEA version is 2021.1 and below, please replace the above steps 4 and 5 by using the
[Save Actions](https://plugins.jetbrains.com/plugin/7642-save-actions) plugin.

## Formatter For Scala

Enable [Scalafmt](https://scalameta.org/scalafmt/) as follows:

1. Go to "Settings/Preferences" → "Editor" → "Code Style" → "Scala"
2. Set "Formatter" to "Scalafmt"
3. Enable "Reformat on file save"

## Checkstyle For Scala

Enable [Scalastyle](http://www.scalastyle.org/) as follows:

1. Go to "Settings/Preferences" → "Editor" → "Inspections".
2. Search for "Scala style inspection" and enable it.

