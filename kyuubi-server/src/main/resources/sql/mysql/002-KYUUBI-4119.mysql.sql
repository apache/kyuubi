SELECT '< KYUUBI-4119: Return app submission time for batch >' AS ' ';

ALTER TABLE metadata ADD COLUMN engine_open_time bigint COMMENT 'the engine open time';
