-- the metadata table ddl

CREATE TABLE IF NOT EXISTS metadata(
    key_id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment key id',
    identifier varchar(36) NOT NULL COMMENT 'the identifier id, which is an UUID',
    session_type varchar(32) NOT NULL COMMENT 'the session type, SQL or BATCH',
    real_user varchar(255) NOT NULL COMMENT 'the real user',
    user_name varchar(255) NOT NULL COMMENT 'the user name, might be a proxy user',
    ip_address varchar(128) COMMENT 'the client ip address',
    kyuubi_instance varchar(1024) COMMENT 'the kyuubi instance that creates this',
    state varchar(128) NOT NULL COMMENT 'the session state',
    resource varchar(1024) COMMENT 'the main resource',
    class_name varchar(1024) COMMENT 'the main class name',
    request_name varchar(1024) COMMENT 'the request name',
    request_conf mediumtext COMMENT 'the request config map',
    request_args mediumtext COMMENT 'the request arguments',
    create_time BIGINT NOT NULL COMMENT 'the metadata create time',
    engine_type varchar(32) NOT NULL COMMENT 'the engine type',
    cluster_manager varchar(128) COMMENT 'the engine cluster manager',
    engine_open_time bigint COMMENT 'the engine open time',
    engine_id varchar(128) COMMENT 'the engine application id',
    engine_name mediumtext COMMENT 'the engine application name',
    engine_url varchar(1024) COMMENT 'the engine tracking url',
    engine_state varchar(32) COMMENT 'the engine application state',
    engine_error mediumtext COMMENT 'the engine application diagnose',
    end_time bigint COMMENT 'the metadata end time',
    priority int NOT NULL DEFAULT 10 COMMENT 'the application priority, high value means high priority',
    peer_instance_closed boolean default '0' COMMENT 'closed by peer kyuubi instance',
    UNIQUE INDEX unique_identifier_index(identifier),
    INDEX user_name_index(user_name),
    INDEX engine_type_index(engine_type),
    INDEX create_time_index(create_time),
    -- See more detail about this index in ./005-KYUUBI-5327.mysql.sql
    INDEX priority_create_time_index(priority DESC, create_time ASC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS k8s_engine_info(
    key_id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment key id',
    identifier varchar(36) NOT NULL COMMENT 'the identifier id, which is an UUID',
    context varchar(32) COMMENT 'the kubernetes context',
    namespace varchar(255) COMMENT 'the kubernetes namespace',
    pod_name varchar(255) NOT NULL COMMENT 'the kubernetes pod name',
    pod_state varchar(32) COMMENT 'the kubernetes pod state',
    container_state mediumtext COMMENT 'the kubernetes container state',
    engine_id varchar(128) COMMENT 'the engine id',
    engine_name mediumtext COMMENT 'the engine name',
    engine_state varchar(32) COMMENT 'the engine state',
    engine_error mediumtext COMMENT 'the engine diagnose',
    update_time bigint COMMENT 'the metadata update time',
    UNIQUE INDEX unique_identifier_index(identifier)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
