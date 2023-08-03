SELECT '< KYUUBI-5131: Create index on metastore.create_time' AS ' ';

CREATE INDEX create_time_index ON metadata(create_time);
