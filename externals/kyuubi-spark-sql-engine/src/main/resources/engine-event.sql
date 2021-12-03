CREATE TABLE IF NOT EXISTS `engine_event_summary` (
  `cluster` varchar(50) DEFAULT NULL COMMENT 'the cluster name for kyuubiServe',
  `app_id` varchar(50) NOT NULL,
  `app_name` varchar(255) NOT NULL,
  `owner` varchar(50) NOT NULL COMMENT 'the application user',
  `share_level` varchar(50) NOT NULL COMMENT 'the share level for this engine',
  `connection_url` varchar(255) NOT NULL COMMENT 'the jdbc connection string',
  `master` varchar(50) NOT NULL COMMENT 'the master type, yarn, k8s, local etc',
  `spark_version` varchar(50) NOT NULL COMMENT 'short version of spark distribution',
  `web_url` varchar(255) NOT NULL COMMENT 'the tracking url of this engine',
  `start_time` bigint(20) NOT NULL COMMENT 'start time',
  `complete_time` bigint(20) DEFAULT '-1' COMMENT 'end time',
  `diagnostic` text DEFAULT NULL COMMENT 'caught exceptions if any',
  `settings` text DEFAULT NULL COMMENT 'collection of all configurations of spark and kyuubi',
  PRIMARY KEY (`app_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `engine_event_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_id` varchar(50) NOT NULL,
  `state` tinyint(1) NOT NULL COMMENT 'the state of the engine',
  `event_time` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `session_event_summary` (
  `cluster` varchar(50) DEFAULT NULL COMMENT 'the cluster name for kyuubiServe',
  `app_id` varchar(50) NOT NULL,
  `session_id` varchar(50) NOT NULL,
  `user_name` varchar(50) NOT NULL,
  `ip` varchar(50) NOT NULL COMMENT 'client ip address',
  `start_time` bigint(20) NOT NULL COMMENT 'start time',
  `complete_time` bigint(20) NOT NULL COMMENT 'end time',
  `total_operations` int DEFAULT '0' COMMENT 'how many queries and meta calls',
  PRIMARY KEY (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `spark_statement_event_summary` (
  `cluster` varchar(50) DEFAULT NULL COMMENT 'the cluster name for kyuubiServe',
  `app_id` varchar(50) NOT NULL,
  `session_id` varchar(50) NOT NULL,
  `statement_id` varchar(50) NOT NULL,
  `statement` text NOT NULL COMMENT 'the sql that you execute',
  `user_name` varchar(50) NOT NULL,
  `create_time` bigint(20) NOT NULL COMMENT 'the create time of this statement',
  `complete_time` bigint(20) NOT NULL COMMENT 'the complete time of this statement',
  `exception_type` varchar(50) DEFAULT NULL COMMENT 'exception type',
  `exception` text DEFAULT NULL COMMENT 'caught exception if have',
  PRIMARY KEY (`statement_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `spark_statement_event_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statement_id` varchar(50) NOT NULL,
  `state` varchar(50) NOT NULL COMMENT 'store each state that the sql has',
  `event_time` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
