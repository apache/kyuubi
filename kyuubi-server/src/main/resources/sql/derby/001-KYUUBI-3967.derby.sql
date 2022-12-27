ALTER TABLE metadata ALTER COLUMN session_type varchar(32) NOT NULL;
ALTER TABLE metadata ALTER COLUMN real_user varchar(255) NOT NULL;
ALTER TABLE metadata ALTER COLUMN user_name varchar(255) NOT NULL;
ALTER TABLE metadata ALTER COLUMN ip_address varchar(128);
ALTER TABLE metadata ALTER COLUMN engine_type varchar(32) NOT NULL;
ALTER TABLE metadata ALTER COLUMN engine_state varchar(32);

DROP INDEX metadata_kyuubi_instance_index;
