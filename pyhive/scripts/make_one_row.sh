#!/bin/bash -eux
hive -e '
set mapred.job.tracker=local;
DROP TABLE IF EXISTS one_row;
CREATE TABLE one_row (number_of_rows INT);
INSERT INTO TABLE one_row VALUES (1);
'
