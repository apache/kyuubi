SELECT '< KYUUBI-5078: Make kyuubi_instance nullable in metadata table schema' AS ' ';

ALTER TABLE metadata MODIFY kyuubi_instance varchar(1024) COMMENT 'the kyuubi instance that creates this';
