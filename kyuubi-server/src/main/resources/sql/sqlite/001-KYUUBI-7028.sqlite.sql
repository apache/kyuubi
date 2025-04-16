-- the kubernetes_metadata table ddl
CREATE TABLE IF NOT EXISTS kubernetes_metadata(
    key_id INTEGER PRIMARY KEY AUTOINCREMENT, -- the auto increment key id
    identifier varchar(36) NOT NULL, -- the identifier id, which is an UUID
    context varchar(32), -- the kubernetes context
    namespace varchar(255), -- the kubernetes namespace
    pod_name varchar(255) NOT NULL, -- the kubernetes pod name
    app_id varchar(128), -- the application id
    app_name mediumtext, -- the application name
    app_state varchar(32), -- the application state
    app_error mediumtext, -- the application diagnose
    create_time bigint, -- the metadata create time
    update_time bigint -- the metadata update time
);

CREATE UNIQUE INDEX IF NOT EXISTS kubernetes_metadata_unique_identifier_index ON kubernetes_metadata(identifier);
