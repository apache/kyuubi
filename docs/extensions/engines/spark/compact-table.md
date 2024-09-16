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

# Compact Table Command Support

It's a new spark sql command to compact small files in a table into larger files, such as 128MB.
After compacting is done, it create a temporary view to query the compacted file details.

Instead of read-write the whole data in a table, it only merges data in the binary and file level,
and it's more efficient.

## syntax

### compact table

```sparksql
compact table table_name [INTO ${targetFileSize} ${targetFileSizeUnit} ] [ cleanup | retain | list ]
-- targetFileSizeUnit can be 'b','k','m','g','t','p'
-- cleanup means cleaning compact staging folders, which contains original small files, default behavior
-- retain means retaining compact staging folders, for testing, and we can recover with the staging data
-- list means this command only get the merging result, and don't run actually
```

### recover table

```sparksql
corecover mpact table table_name
-- recover the compacted table, and restore the small files from staging to the original location
```

## example

The following command will compact the small files in the table `default.small_files_table` into 128MB files, and create
a temporary view `v_merged_files` to query the compacted file details.

```sparksql
set spark.sql.shuffle.partitions=32;

compact table default.small_files_table;

select * from v_merged_files;
```

## Supported table format

| Table Format |   Codec    | Supported |
|--------------|------------|-----------|
| parquet      | all codecs | Y         |
| avro         | snappy     | Y         |
| json         | gzip       | Y         |
| csv          | gzip       | Y         |
| text         | gzip       | Y         |
| orc          | snappy     | Y         |

