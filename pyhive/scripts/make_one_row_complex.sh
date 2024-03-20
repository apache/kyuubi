#!/bin/bash -eux

COLUMNS='
`boolean` BOOLEAN,
`tinyint` TINYINT,
`smallint` SMALLINT,
`int` INT,
`bigint` BIGINT,
`float` FLOAT,
`double` DOUBLE,
`string` STRING,
`timestamp` TIMESTAMP,
`binary` BINARY,
`array` ARRAY<int>,
`map` MAP<int, int>,
`struct` STRUCT<a: int, b: int>,
`union` UNIONTYPE<int, string>,
`decimal` DECIMAL(10, 1)
'

hive -e "
set mapred.job.tracker=local;
DROP TABLE IF EXISTS one_row_complex;
DROP TABLE IF EXISTS one_row_complex_null;
CREATE TABLE one_row_complex ($COLUMNS);
CREATE TABLE one_row_complex_null ($COLUMNS);
INSERT OVERWRITE TABLE one_row_complex SELECT
    true,
    127,
    32767,
    2147483647,
    9223372036854775807,
    0.5,
    0.25,
    'a string',
    0,
    '123',
    array(1, 2),
    map(1, 2, 3, 4),
    named_struct('a', 1, 'b', 2),
    create_union(0, 1, 'test_string'),
    0.1
FROM one_row;
INSERT OVERWRITE TABLE one_row_complex_null SELECT
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    IF(false, array(1, 2), null),
    IF(false, map(1, 2, 3, 4), null),
    IF(false, named_struct('a', 1, 'b', 2), null),
    IF(false, create_union(0, 1, 'test_string'), null),
    null
FROM one_row;
"

# Note: using IF(false, ...) above to work around https://issues.apache.org/jira/browse/HIVE-4022
# The problem is that a "void" type cannot be inserted into a complex type field.
