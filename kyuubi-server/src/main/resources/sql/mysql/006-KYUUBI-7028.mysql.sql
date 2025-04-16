SELECT '< KYUUBI-7028: Persist Kubernetes metadata into metastore' AS ' ';

CREATE TABLE IF NOT EXISTS kubernetes_metadata(
    key_id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment key id',
    identifier varchar(36) NOT NULL COMMENT 'the identifier id, which is an UUID',
    context varchar(32) COMMENT 'the kubernetes context',
    namespace varchar(255) COMMENT 'the kubernetes namespace',
    pod_name varchar(255) NOT NULL COMMENT 'the kubernetes pod name',
    app_id varchar(128) COMMENT 'the application id',
    app_state varchar(32) COMMENT 'the application state',
    app_error mediumtext COMMENT NOT NULL COMMENT 'the application diagnose',
    create_time bigint COMMENT 'the metadata create time',
    update_time bigint COMMENT 'the metadata update time',
    UNIQUE INDEX unique_identifier_index(identifier)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
