SELECT '< Upgrading MetaStore schema from 1.6.0 to 1.7.0 >' AS ' ';
.read 001-KYUUBI-3967.sqlite.sql
.read 002-KYUUBI-4119.sqlite.sql
SELECT '< Finished upgrading MetaStore schema from 1.6.0 to 1.7.0 >' AS ' ';
