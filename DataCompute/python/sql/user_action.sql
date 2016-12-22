CREATE TABLE `user_action_%s` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `uid` int(11) unsigned NOT NULL,
  `p_type` varchar(1) DEFAULT '0' COMMENT '产品类型：0minik,1kshow',
  `t_type` varchar(1) DEFAULT '0' COMMENT '终端类型:0实体机，1移动端',
  `a_type` varchar(1) DEFAULT '0' COMMENT '行为类型',
  `session_id` int(11) DEFAULT NULL,
  `action_time` int(11) unsigned NOT NULL,
  `location` int(11) unsigned NOT NULL,
  `aimei_object` varchar(256) DEFAULT '',
  `aimei_value` varchar(512) DEFAULT NULL,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `key_uid` (`uid`) USING BTREE,
  KEY `time_mid` (`action_time`,`location`)
) ENGINE=InnoDB AUTO_INCREMENT=12376472 DEFAULT CHARSET=utf8;