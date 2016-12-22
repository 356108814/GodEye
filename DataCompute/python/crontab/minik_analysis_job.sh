#!/bin/bash
# 每天凌晨4点默认处理前一天的日志
yesterday=`date -d yesterday +%Y-%m-%d`
year=${yesterday:0:4}

cd /var/lib/hadoop-hdfs/DataCompute/python

./submit-spark-job minik_game_session.py ${yesterday}
./submit-spark-job minik_song_play.py ${yesterday}
./submit-spark-job minik_user_action.py ${yesterday}
./submit-spark-job minik_weixin_user_action.py ${yesterday}

# 定期统计任务统计入口
./submit-spark-job minik_count_from_db.py ${yesterday}