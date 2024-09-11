# kyuubi-extension-spark-3-3

## compact table command

it's a new spark sql command to compact small files in a table into larger files, such as 128MB. After compacting is
done, it create a temporary view to query the compacted file details.

### syntax

```sparksql
compact table table_name [INTO ${targetFileSize} ${targetFileSizeUnit} ] [ cleanup | retain | list ]
-- targetFileSizeUnit can be 'b','k','m','g','t','p'
-- cleanup means cleaning compact staging folders, which contains original small files, default behavior
-- retain means retaining compact staging folders, for testing, and we can recover with the staging data
-- list means this command only get the merging result, and don't run actually
```

### example

The following command will compact the small files in the table `default.small_files_table` into 128MB files, and create
a temporary view `v_merged_files` to query the compacted file details.

```sparksql
set spark.sql.shuffle.partitions=32;

compact table default.small_files_table;

select * from v_merged_files;
```