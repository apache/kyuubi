-- the metadata table ddl

CREATE TABLE metadata(
    key_id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment key id',
    identifier varchar(36) NOT NULL COMMENT 'the identifier id, which is an UUID',
    session_type varchar(128) NOT NULL COMMENT 'the session type, SQL or BATCH',
    real_user varchar(1024) NOT NULL COMMENT 'the real user',
    user_name varchar(1024) NOT NULL COMMENT 'the user name, might be a proxy user',
    ip_address varchar(512) COMMENT 'the client ip address',
    kyuubi_instance varchar(1024) NOT NULL COMMENT 'the kyuubi instance that creates this',
    state varchar(128) NOT NULL COMMENT 'the session state',
    resource varchar(1024) COMMENT 'the main resource',
    class_name varchar(1024) COMMENT 'the main class name',
    request_name varchar(1024) COMMENT 'the request name',
    request_conf mediumtext COMMENT 'the request config map',
    request_args mediumtext COMMENT 'the request arguments',
    create_time BIGINT NOT NULL COMMENT 'the metadata create time',
    engine_type varchar(1024) NOT NULL COMMENT 'the engine type',
    cluster_manager varchar(128) COMMENT 'the engine cluster manager',
    engine_id varchar(128) COMMENT 'the engine application id',
    engine_name mediumtext COMMENT 'the engine application name',
    engine_url varchar(1024) COMMENT 'the engine tracking url',
    engine_state varchar(128) COMMENT 'the engine application state',
    engine_error mediumtext COMMENT 'the engine application diagnose',
    end_time bigint COMMENT 'the metadata end time',
    peer_instance_closed boolean default '0' COMMENT 'closed by peer kyuubi instance',
    INDEX kyuubi_instance_index(kyuubi_instance),
    UNIQUE INDEX unique_identifier_index(identifier),
    INDEX user_name_index(user_name),
    INDEX engine_type_index(engine_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `t_sys_user` (
                              `id` bigint(20) NOT NULL AUTO_INCREMENT,
                              `group_id` bigint(20) DEFAULT NULL COMMENT 'group id',
                              `name` varchar(255) NOT NULL COMMENT 'user name',
                              `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
                              `remark` varchar(500) NOT NULL COMMENT 'remark',
                              PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `t_limit_user` (
                                `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                `user_id` bigint(20) NOT NULL COMMENT 'user id',
                                `num` int(20) NOT NULL COMMENT 'number of limit value',
                                `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
                                `remark` varchar(500) NOT NULL COMMENT 'remark',
                                PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `t_sys_group` (
                               `id` bigint(20) NOT NULL AUTO_INCREMENT,
                               `name` varchar(255) NOT NULL COMMENT 'group name',
                               `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
                               `remark` varchar(500) NOT NULL COMMENT 'remark',
                               PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;