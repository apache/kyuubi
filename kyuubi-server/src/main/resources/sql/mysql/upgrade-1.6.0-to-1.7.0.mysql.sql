SELECT '< Upgrading MetaStore schema from 1.6.0 to 1.7.0 >' AS ' ';
SOURCE 001-KYUUBI-3967.mysql.sql;
SOURCE 002-KYUUBI-4119.mysql.sql;
SELECT '< Finished upgrading MetaStore schema from 1.6.0 to 1.7.0 >' AS ' ';
