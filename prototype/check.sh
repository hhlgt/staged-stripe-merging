#!/bin/bash
set -e
# '10.0.0.14'
ARRAY=('10.0.0.2' '10.0.0.3' '10.0.0.5' '10.0.0.6' '10.0.0.7' '10.0.0.8' '10.0.0.9' '10.0.0.10' '10.0.0.11' 
        '10.0.0.12' '10.0.0.13' '10.0.0.15' '10.0.0.16' '10.0.0.17' '10.0.0.18')
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`

for i in $(seq 0 $NUM)
do
temp=${ARRAY[$i]}
    echo $temp
    ssh widestripe@$temp 'ps -aux | grep run_datanode | wc -l'
    ssh widestripe@$temp 'ps -aux | grep run_proxy | wc -l'
done