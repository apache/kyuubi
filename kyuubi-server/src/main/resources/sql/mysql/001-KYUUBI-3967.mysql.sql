SELECT '< KYUUBI-3967: Shorten column varchar length of metadata table >' AS ' ';

ALTER TABLE metadata MODIFY COLUMN session_type varchar(32) NOT NULL COMMENT 'the session type, SQL or BATCH';
ALTER TABLE metadata MODIFY COLUMN real_user varchar(255) NOT NULL COMMENT 'the real user';
ALTER TABLE metadata MODIFY COLUMN user_name varchar(255) NOT NULL COMMENT 'the user name, might be a proxy user';
ALTER TABLE metadata MODIFY COLUMN ip_address varchar(128) COMMENT 'the client ip address';
ALTER TABLE metadata MODIFY COLUMN engine_type varchar(32) NOT NULL COMMENT 'the engine type';
ALTER TABLE metadata MODIFY COLUMN engine_state varchar(32) COMMENT 'the engine application state';

DROP INDEX kyuubi_instance_index on metadata;
