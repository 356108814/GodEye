#!/bin/bash

for id in 14 15 16 17 18
do
    ./put_minik_log_to_hdfs.sh 2016-09-$id
done
