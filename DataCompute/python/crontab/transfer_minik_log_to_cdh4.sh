#!/bin/bash
# 在102上凌晨2点将指定日期（默认前一天）的日志传输到CDH-4上供集群处理
PARAM_DATE=$1
if [ $PARAM_DATE ];then
    yesterday=$PARAM_DATE
else
    yesterday=`date -d yesterday +%Y-%m-%d`
fi

year=${yesterday:0:4}

# 源
src_minik_machine_user_action=/diskb/aimei_data_log_logstash/minik_machine/user_action/${year}/${yesterday}.log
src_minik_server_logic=/diskb/aimei_data_log_logstash/minik_server/logic_log/${year}/${yesterday}.log
src_minik_weixin_user_action=/diskb/aimei_data_log_logstash/minik_weixin/user_action/${year}/${yesterday}.log

# scp传输
scp_base=root@CDH-4:/data/disk1/logdata
scp ${src_minik_machine_user_action} ${scp_base}/minik_machine/user_action/${year}
scp ${src_minik_server_logic} ${scp_base}/minik_server/logic/${year}
scp ${src_minik_weixin_user_action} ${scp_base}/minik_weixin/user_action/${year} 
