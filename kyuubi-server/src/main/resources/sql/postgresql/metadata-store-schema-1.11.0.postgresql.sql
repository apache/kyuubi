CREATE TABLE IF NOT EXISTS metadata(
    key_id bigserial PRIMARY KEY,
    identifier varchar(36) NOT NULL,
    session_type varchar(32) NOT NULL,
    real_user varchar(255) NOT NULL,
    user_name varchar(255) NOT NULL,
    ip_address varchar(128),
    kyuubi_instance varchar(1024),
    state varchar(128) NOT NULL,
    resource varchar(1024),
    class_name varchar(1024),
    request_name varchar(1024),
    request_conf text,
    request_args text,
    create_time bigint NOT NULL,
    engine_type varchar(32) NOT NULL,
    cluster_manager varchar(128),
    engine_open_time bigint,
    engine_id varchar(128),
    engine_name text,
    engine_url varchar(1024),
    engine_state varchar(32),
    engine_error text,
    end_time bigint,
    priority int NOT NULL DEFAULT 10,
    peer_instance_closed boolean DEFAULT FALSE
);

COMMENT ON COLUMN metadata.key_id IS 'the auto increment key id';
COMMENT ON COLUMN metadata.identifier IS 'the identifier id, which is an UUID';
COMMENT ON COLUMN metadata.session_type IS 'the session type, SQL or BATCH';
COMMENT ON COLUMN metadata.real_user IS 'the real user';
COMMENT ON COLUMN metadata.user_name IS 'the user name, might be a proxy user';
COMMENT ON COLUMN metadata.ip_address IS 'the client ip address';
COMMENT ON COLUMN metadata.kyuubi_instance IS 'the kyuubi instance that creates this';
COMMENT ON COLUMN metadata.state IS 'the session state';
COMMENT ON COLUMN metadata.resource IS 'the main resource';
COMMENT ON COLUMN metadata.class_name IS 'the main class name';
COMMENT ON COLUMN metadata.request_name IS 'the request name';
COMMENT ON COLUMN metadata.request_conf IS 'the request config map';
COMMENT ON COLUMN metadata.request_args IS 'the request arguments';
COMMENT ON COLUMN metadata.create_time IS 'the metadata create time';
COMMENT ON COLUMN metadata.engine_type IS 'the engine type';
COMMENT ON COLUMN metadata.cluster_manager IS 'the engine cluster manager';
COMMENT ON COLUMN metadata.engine_open_time IS 'the engine open time';
COMMENT ON COLUMN metadata.engine_id IS 'the engine application id';
COMMENT ON COLUMN metadata.engine_name IS 'the engine application name';
COMMENT ON COLUMN metadata.engine_url IS 'the engine tracking url';
COMMENT ON COLUMN metadata.engine_state IS 'the engine application state';
COMMENT ON COLUMN metadata.engine_error IS 'the engine application diagnose';
COMMENT ON COLUMN metadata.end_time IS 'the metadata end time';
COMMENT ON COLUMN metadata.priority IS 'the application priority, high value means high priority';
COMMENT ON COLUMN metadata.peer_instance_closed IS 'closed by peer kyuubi instance';

CREATE UNIQUE INDEX IF NOT EXISTS unique_identifier_index ON metadata(identifier);
CREATE INDEX IF NOT EXISTS user_name_index ON metadata(user_name);
CREATE INDEX IF NOT EXISTS engine_type_index ON metadata(engine_type);
CREATE INDEX IF NOT EXISTS create_time_index ON metadata(create_time);
CREATE INDEX IF NOT EXISTS priority_create_time_index ON metadata(priority DESC, create_time ASC);

CREATE TABLE IF NOT EXISTS k8s_engine_info(
    key_id bigserial PRIMARY KEY,
    identifier varchar(36) NOT NULL,
    context varchar(32),
    namespace varchar(255),
    pod_name varchar(255) NOT NULL,
    pod_state varchar(32),
    container_state text,
    engine_id varchar(128),
    engine_name text,
    engine_state varchar(32),
    engine_error text,
    create_time bigint NOT NULL,
    update_time bigint NOT NULL
);

COMMENT ON COLUMN k8s_engine_info.key_id IS 'the auto increment key id';
COMMENT ON COLUMN k8s_engine_info.identifier IS 'the identifier id, which is an UUID';
COMMENT ON COLUMN k8s_engine_info.context IS 'the kubernetes context';
COMMENT ON COLUMN k8s_engine_info.namespace IS 'the kubernetes namespace';
COMMENT ON COLUMN k8s_engine_info.pod_name IS 'the kubernetes pod name';
COMMENT ON COLUMN k8s_engine_info.pod_state IS 'the kubernetes pod state';
COMMENT ON COLUMN k8s_engine_info.container_state IS 'the kubernetes container state';
COMMENT ON COLUMN k8s_engine_info.engine_id IS 'the engine id';
COMMENT ON COLUMN k8s_engine_info.engine_name IS 'the engine name';
COMMENT ON COLUMN k8s_engine_info.engine_state IS 'the engine state';
COMMENT ON COLUMN k8s_engine_info.engine_error IS 'the engine diagnose';
COMMENT ON COLUMN k8s_engine_info.create_time IS 'the metadata create time';
COMMENT ON COLUMN k8s_engine_info.update_time IS 'the metadata update time';

CREATE UNIQUE INDEX IF NOT EXISTS k8s_engine_info_unique_identifier_index ON k8s_engine_info(identifier);
