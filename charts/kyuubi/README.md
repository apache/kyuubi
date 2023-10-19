<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Helm Chart for Apache Kyuubi

[Apache Kyuubi](https://kyuubi.apache.org) is a distributed and multi-tenant gateway to provide serverless SQL on Data Warehouses and Lakehouses.


## Introduction

This chart will bootstrap an [Kyuubi](https://kyuubi.apache.org) deployment on a [Kubernetes](http://kubernetes.io)
cluster using the [Helm](https://helm.sh) package manager.

## Requirements

- Kubernetes cluster
- Helm 3.0+

## Template rendering

When you want to test the template rendering, but not actually install anything. [Debugging templates](https://helm.sh/docs/chart_template_guide/debugging/) provide a quick way of viewing the generated content without YAML parse errors blocking.

There are two ways to render templates. It will return the rendered template to you so you can see the output.

- Local rendering chart templates
```shell
helm template --debug ../kyuubi
```
- Server side rendering chart templates
```shell
helm install --dry-run --debug --generate-name ../kyuubi
```
<!-- ## Features -->

## Documentation

Configuration guide documentation for Kyuubi lives [on the website](https://kyuubi.readthedocs.io/en/master/configuration/settings.html#kyuubi-configurations). (Not just for Helm Chart)

## Contributing

Want to help build Apache Kyuubi? Check out our [contributing documentation](https://kyuubi.readthedocs.io/en/master/community/CONTRIBUTING.html).