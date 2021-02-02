#!/bin/sh

#Usage:
# ./loop_to_test_sm.sh 100
# 100 -> how many times you want to execute

for ((i = 1; i <= $1; i++)); do
    start=$(date +%s)
    go test -race > shardmaster_out

    if [ $? -ne 0 ]; then
      end=$(date +%s)
      take=$(( end - start ))
      echo "shardmaster test $i failed, take time ${take}s"
      break
    else
      end=$(date +%s)
      take=$(( end - start ))
      echo "shardmaster test $i succeed, take time ${take}s"
    fi
    sleep 0.5
done
