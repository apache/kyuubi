CREATE TABLE IF NOT EXISTS `engine_event_summary` (
  `cluster` varchar(50) DEFAULT NULL COMMENT 'kyuubiServe集群名称，kyuubi展示服务可以管理多个集群的kyuubiServer',
  `app_id` varchar(50) NOT NULL,
  `app_name` varchar(255) NOT NULL,
  `owner` varchar(50) NOT NULL COMMENT 'app所属人员',
  `share_level` varchar(50) NOT NULL COMMENT 'engine的共享级别',
  `connection_url` varchar(255) NOT NULL COMMENT 'jdbc连接串',
  `master` varchar(50) NOT NULL COMMENT 'master类型:yarn,k8s,local等',
  `spark_version` varchar(50) NOT NULL COMMENT 'spark版本',
  `web_url` varchar(255) NOT NULL,
  `start_time` bigint(20) NOT NULL COMMENT 'engine开始时间',
  `complete_time` bigint(20) DEFAULT '-1' COMMENT 'engine结束时间',
  `diagnostic` text DEFAULT NULL COMMENT '诊断信息',
  `settings` text DEFAULT NULL COMMENT 'spark和kyuubi的所有配置项',
  PRIMARY KEY (`app_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `engine_event_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_id` varchar(50) NOT NULL,
  `state` tinyint(1) NOT NULL COMMENT 'engine状态',
  `event_time` bigint(20) NOT NULL COMMENT 'engine变成此状态的时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `session_event_summary` (
  `cluster` varchar(50) DEFAULT NULL COMMENT 'kyuubiServer集群名称，kyuubi展示服务可以管理多个集群的kyuubiServer',
  `app_id` varchar(50) NOT NULL,
  `session_id` varchar(50) NOT NULL,
  `user_name` varchar(50) NOT NULL,
  `ip` varchar(50) NOT NULL,
  `start_time` bigint(20) NOT NULL COMMENT 'session开始时间',
  `complete_time` bigint(20) NOT NULL COMMENT 'session结束时间',
  `total_operations` int DEFAULT '0' COMMENT 'session上执行的query总数',
  PRIMARY KEY (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `spark_statement_event_summary` (
  `cluster` varchar(50) DEFAULT NULL COMMENT 'kyuubiServe集群名称，kyuubi展示服务可以管理多个集群的kyuubiServer',
  `app_id` varchar(50) NOT NULL,
  `session_id` varchar(50) NOT NULL,
  `statement_id` varchar(50) NOT NULL,
  `statement` text NOT NULL COMMENT 'statement详情',
  `user_name` varchar(50) NOT NULL,
  `create_time` bigint(20) NOT NULL COMMENT 'statement创建时间',
  `complete_time` bigint(20) NOT NULL COMMENT 'statement结束时间',
  `exception_type` varchar(50) DEFAULT NULL COMMENT '异常类型',
  `exception` text DEFAULT NULL COMMENT '异常详情',
  PRIMARY KEY (`statement_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `spark_statement_event_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statement_id` varchar(50) NOT NULL,
  `state` varchar(50) NOT NULL COMMENT 'statement状态',
  `event_time` bigint(20) NOT NULL COMMENT 'statement变成此状态的时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
