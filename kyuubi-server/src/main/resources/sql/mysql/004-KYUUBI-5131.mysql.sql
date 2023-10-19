SELECT '< KYUUBI-5131: Create index on metastore.create_time' AS ' ';

ALTER TABLE metadata ADD INDEX create_time_index(create_time);
