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

```properties

## Shuffle Behavior
# spark.shuffle.compress                                    true
# spark.shuffle.detectCorrupt                               true
# spark.shuffle.detectCorrupt.useExtraMemory                true
# spark.shuffle.file.buffer                                 64k
# spark.shuffle.unsafe.file.output.buffer                   64k
# spark.shuffle.spill.diskWriteBufferSize                   8k
# spark.shuffle.spill.compress                              true
# spark.shuffle.mapOutput.dispatcher.numThreads             12
# spark.shuffle.mapOutput.parallelAggregationThreshold      5000
# spark.shuffle.readHostLocalDisk                           true
# spark.shuffle.io.maxRetries                               10
# spark.shuffle.io.retryWait                                6s
# spark.shuffle.io.preferDirectBufs                         false
# spark.shuffle.io.serverThreads                            8
# spark.shuffle.io.clientThreads                            8
# spark.shuffle.io.connectionTimeout                        240s
# spark.shuffle.registration.timeout                        6000
# spark.shuffle.registration.maxAttempts                    10
# spark.shuffle.sync                                        false
# spark.shuffle.useOldFetchProtocol                         true
# spark.shuffle.unsafe.fastMergeEnabled                     true
# spark.shuffle.minNumPartitionsToHighlyCompress            100
# spark.network.maxRemoteBlockSizeFetchToMem                128m
# spark.reducer.maxSizeInFlight                             48m
# spark.reducer.maxReqsInFlight                             256
# spark.reducer.maxBlocksInFlightPerAddress                 256
```