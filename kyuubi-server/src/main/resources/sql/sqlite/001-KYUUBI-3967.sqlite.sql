SELECT '< KYUUBI-3967: Shorten column varchar length of metadata table >' AS ' ';

BEGIN;

CREATE TABLE metadata_X(
    key_id INTEGER PRIMARY KEY AUTOINCREMENT, -- the auto increment key id
    identifier varchar(36) NOT NULL, -- the identifier id, which is an UUID
    session_type varchar(32) NOT NULL, -- the session type, SQL or BATCH
    real_user varchar(255) NOT NULL, -- the real user
    user_name varchar(255) NOT NULL, -- the user name, might be a proxy user
    ip_address varchar(128), -- the client ip address
    kyuubi_instance varchar(1024) NOT NULL, -- the kyuubi instance that creates this
    state varchar(128) NOT NULL, -- the session state
    resource varchar(1024), -- the main resource
    class_name varchar(1024), -- the main class name
    request_name varchar(1024), -- the request name
    request_conf mediumtext, -- the request config map
    request_args mediumtext, -- the request arguments
    create_time BIGINT NOT NULL, -- the metadata create time
    engine_type varchar(32) NOT NULL, -- the engine type
    cluster_manager varchar(128), -- the engine cluster manager
    engine_id varchar(128), -- the engine application id
    engine_name mediumtext, -- the engine application name
    engine_url varchar(1024), -- the engine tracking url
    engine_state varchar(32), -- the engine application state
    engine_error mediumtext, -- the engine application diagnose
    end_time bigint, -- the metadata end time
    peer_instance_closed boolean default '0' -- closed by peer kyuubi instance
);

INSERT INTO metadata_X SELECT * FROM metadata;

DROP TABLE metadata;

ALTER TABLE metadata_X RENAME TO metadata;

CREATE INDEX metadata_kyuubi_instance_index ON metadata(kyuubi_instance);

CREATE UNIQUE INDEX metadata_unique_identifier_index ON metadata(identifier);

CREATE INDEX metadata_user_name_index ON metadata(user_name);

CREATE INDEX metadata_engine_type_index ON metadata(engine_type);

COMMIT;

DROP INDEX metadata_kyuubi_instance_index;
