#!/bin/bash
# 合并前一天的日志然后传到CDH-4上供集群处理
yesterday=`date -d yesterday +%Y-%m-%d`
year=${yesterday:0:4}

# 源
src_minik_machine_user_action=/diskb/aimei_data_log/minik_machine/user_action/new/$year/$yesterday
src_minik_server_logic=/diskb/aimei_data_log/minik_server/logic_log2/$year/$yesterday
src_minik_weixin_user_action=/diskb/aimei_data_log/minik_weixin/user_action2/$year/$yesterday

# 目标位置
dest_base=/diskb/merge_2_mv_log
dest_minik_machine_user_action=${dest_base}/minik_machine/user_action/${year}/${yesterday}.log
dest_minik_server_logic=${dest_base}/minik_server/logic/${year}/${yesterday}.log
dest_minik_weixin_user_action=${dest_base}/minik_weixin/user_action/${year}/${yesterday}.log

# 删除源的.gz文件
rm -f ${src_minik_machine_user_action}/*.gz
rm -f ${src_minik_server_logic}/*/*.gz
rm -f ${src_minik_weixin_user_action}/*/*.gz

# 合并
cat ${src_minik_machine_user_action}/* >> ${dest_minik_machine_user_action}
cat ${src_minik_server_logic}/*/* >> ${dest_minik_server_logic}
cat ${src_minik_weixin_user_action}/*/* >> ${dest_minik_weixin_user_action}

# scp传输
scp_base=root@CDH-4:/data/disk1/logdata
scp ${dest_minik_machine_user_action} ${scp_base}/minik_machine/user_action/${year}
scp ${dest_minik_server_logic} ${scp_base}/minik_server/logic/${year}
scp ${dest_minik_weixin_user_action} ${scp_base}/minik_weixin/user_action/${year} 
