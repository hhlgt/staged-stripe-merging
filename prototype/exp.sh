#!/bin/bash
set -e

# ARRAY=('10.0.0.2' '10.0.0.3' '10.0.0.5' '10.0.0.6' '10.0.0.7' '10.0.0.8' '10.0.0.9' '10.0.0.10' '10.0.0.11' 
#         '10.0.0.12' '10.0.0.13' '10.0.0.14' '10.0.0.15' '10.0.0.16' '10.0.0.17' '10.0.0.18')
ARRAY=('10.0.0.2' '10.0.0.3' '10.0.0.5' '10.0.0.6' '10.0.0.7' '10.0.0.8' '10.0.0.9' '10.0.0.10' '10.0.0.11' 
        '10.0.0.12' '10.0.0.13' '10.0.0.15') # else
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`
SRC_PATH1=/home/widestripe/staged_stripe_merging/prototype/run_cluster_sh/
SRC_PATH2=/home/widestripe/staged_stripe_merging/prototype/project
SRC_PATH3=/home/widestripe/wondershaper
SRC_PATH4=/home/widestripe/staged_stripe_merging/prototype/kill_proxy_datanode.sh

# DIR_NAME=run_memcached
DIS_DIR1=/home/widestripe/staged_stripe_merging/prototype
DIS_DIR2=/home/widestripe/staged_stripe_merging/prototype/storage
DIS_DIR3=/home/widestripe/wondershaper

for i in $(seq 0 $NUM)
do
temp=${ARRAY[$i]}
    echo $temp
    if [ $1 == 0 ]; then
        ssh widestripe@$temp 'pkill -9 run_datanode;pkill -9 run_proxy'
        echo 'pkill  all'
        ssh widestripe@$temp 'ps -aux | grep run_datanode | wc -l'
        ssh widestripe@$temp 'ps -aux | grep run_proxy | wc -l'
    elif [ $1 == 1 ]; then
        ssh widestripe@$temp 'cd /home/widestripe/staged_stripe_merging/prototype;bash cluster_run_proxy_datanode.sh'
        echo 'proxy_datanode process number:'
        ssh widestripe@$temp 'ps -aux |grep run_datanode | wc -l;ps -aux |grep run_proxy | wc -l'
    elif [ $1 == 2 ]; then
        ssh widestripe@$temp 'mkdir -p' ${DIS_DIR1}
        ssh widestripe@$temp 'mkdir -p' ${DIS_DIR2}
        ssh widestripe@$temp 'mkdir -p' ${DIS_DIR3}
        rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_proxy_datanode.sh widestripe@$temp:${DIS_DIR1}
        rsync -rtvpl ${SRC_PATH2} widestripe@$temp:${DIS_DIR1}
        rsync -rtvpl ${SRC_PATH3} widestripe@$temp:${DIS_DIR3}
        rsync -rtvpl ${SRC_PATH4} widestripe@$temp:${DIS_DIR1}
    elif [ $1 == 3 ]; then
        ssh widestripe@$temp 'sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 986710'
    elif [ $1 == 4 ]; then
        ssh widestripe@$temp 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0'
    elif [ $1 == 5 ]; then
        ssh widestripe@$temp 'cd /home/widestripe/staged_stripe_merging/prototype/storage/;rm -rf *'
    fi
done