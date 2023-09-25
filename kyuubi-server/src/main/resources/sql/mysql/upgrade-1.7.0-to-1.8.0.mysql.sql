SELECT '< Upgrading MetaStore schema from 1.7.0 to 1.8.0 >' AS ' ';
SOURCE 003-KYUUBI-5078.mysql.sql;
SOURCE 004-KYUUBI-5131.mysql.sql;
SOURCE 005-KYUUBI-5327.mysql.sql;
SELECT '< Finished upgrading MetaStore schema from 1.7.0 to 1.8.0 >' AS ' ';
