SELECT '< KYUUBI-7028: Persist Kubernetes metadata into metastore' AS ' ';

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
