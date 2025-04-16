-- the k8s_engine_info table ddl
CREATE TABLE IF NOT EXISTS k8s_engine_info(
    key_id INTEGER PRIMARY KEY AUTOINCREMENT, -- the auto increment key id
    identifier varchar(36) NOT NULL, -- the identifier id, which is an UUID
    context varchar(32), -- the kubernetes context
    namespace varchar(255), -- the kubernetes namespace
    pod_name varchar(255) NOT NULL, -- the kubernetes pod name
    pod_state varchar(32), -- the kubernetes pod state
    container_state mediumtext, -- the kubernetes container state
    engine_id varchar(128), -- the engine id
    engine_name mediumtext, -- the engine name
    engine_state varchar(32), -- the engine state
    engine_error mediumtext, -- the engine diagnose
    create_time bigint, -- the metadata create time
    update_time bigint -- the metadata update time
);

CREATE UNIQUE INDEX IF NOT EXISTS k8s_engine_info_unique_identifier_index ON k8s_engine_info(identifier);
