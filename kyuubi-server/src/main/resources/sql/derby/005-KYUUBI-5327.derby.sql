ALTER TABLE metadata ADD COLUMN priority int default 10;

CREATE INDEX metadata_priority_create_time_index ON metadata(priority, create_time);
