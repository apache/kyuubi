SELECT '< KYUUBI-5327: Introduce priority in metadata' AS ' ';

ALTER TABLE metadata ADD COLUMN priority int NOT NULL DEFAULT 10 COMMENT 'the application priority, high value means high priority';

-- In MySQL 5.7, A key_part specification can end with ASC or DESC.
-- These keywords are permitted for future extensions for specifying ascending or descending index value storage.
-- Currently, they are parsed but ignored; index values are always stored in ascending order.
-- In MySQL 8 this can take effect and this index will be hit if query order by priority DESC, create_time ASC.
-- See more detail in:
-- https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
-- https://dev.mysql.com/doc/refman/8.0/en/create-index.html
-- https://dev.mysql.com/doc/refman/5.7/en/create-index.html
ALTER TABLE metadata ADD INDEX priority_create_time_index(priority DESC, create_time ASC);
