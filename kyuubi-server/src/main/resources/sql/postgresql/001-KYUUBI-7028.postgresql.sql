SELECT '< KYUUBI-7028: Persist Kubernetes metadata into metastore' AS ' ';

CREATE TABLE IF NOT EXISTS kubernetes_metadata(
    key_id bigserial PRIMARY KEY,
    identifier varchar(36) NOT NULL,
    context varchar(32),
    namespace varchar(255),
    pod_name varchar(255) NOT NULL,
    app_id varchar(128),
    app_name text,
    app_state varchar(32),
    app_error text,
    create_time bigint NOT NULL,
    update_time bigint NOT NULL
    );

COMMENT ON COLUMN kubernetes_metadata.key_id IS 'the auto increment key id';
COMMENT ON COLUMN kubernetes_metadata.identifier IS 'the identifier id, which is an UUID';
COMMENT ON COLUMN kubernetes_metadata.context IS 'the kubernetes context';
COMMENT ON COLUMN kubernetes_metadata.namespace IS 'the kubernetes namespace';
COMMENT ON COLUMN kubernetes_metadata.pod_name IS 'the kubernetes pod name';
COMMENT ON COLUMN kubernetes_metadata.app_id IS 'the application id';
COMMENT ON COLUMN kubernetes_metadata.app_name IS 'the application name';
COMMENT ON COLUMN kubernetes_metadata.app_state IS 'the application state';
COMMENT ON COLUMN kubernetes_metadata.app_error IS 'the application diagnose';
COMMENT ON COLUMN kubernetes_metadata.create_time IS 'the metadata create time';
COMMENT ON COLUMN kubernetes_metadata.update_time IS 'the metadata update time';

CREATE UNIQUE INDEX IF NOT EXISTS kubernetes_metadata_unique_identifier_index ON kubernetes_metadata(identifier);
