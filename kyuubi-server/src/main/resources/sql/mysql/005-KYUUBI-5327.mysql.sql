SELECT '< KYUUBI-5131: Create index on metastore.create_time' AS ' ';

ALTER TABLE metadata ADD COLUMN priority int default 10 COMMENT 'the application priority, high value means high priority';

-- mysql 5.7 does not support mix order index
-- mysql 8 allow create mix order index and can be used in order by
-- here create it in 5.7 won't cause error
ALTER TABLE metadata ADD INDEX priority_create_time_index(priority DESC, create_time ASC);
