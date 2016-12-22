#!/bin/bash
# 在CDH-4上，使用hdfs用户，凌晨3点将指定日期（默认前一天）的日志put到hdfs
PARAM_DATE=$1
if [ $PARAM_DATE ];then
    yesterday=$PARAM_DATE
else
    yesterday=`date -d yesterday +%Y-%m-%d`
fi

year=${yesterday:0:4}

# 文件模板
tpl_minik_machine_user_action=minik_machine/user_action/${year}
tpl_minik_server_logic=minik_server/logic/${year}
tpl_minik_weixin_user_action=minik_weixin/user_action/${year}

# put to hdfs Hadoop目录结构和当前的一致
path_base=/data/disk1/logdata
hadoop fs -put ${path_base}/${tpl_minik_machine_user_action}/${yesterday}.log ${path_base}/${tpl_minik_machine_user_action}/ 
hadoop fs -put ${path_base}/${tpl_minik_server_logic}/${yesterday}.log ${path_base}/${tpl_minik_server_logic}/ 
hadoop fs -put ${path_base}/${tpl_minik_weixin_user_action}/${yesterday}.log ${path_base}/${tpl_minik_weixin_user_action}/ 
