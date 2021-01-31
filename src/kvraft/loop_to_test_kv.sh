#!/bin/sh

#Usage:
# ./loop_to_test_raft.sh TestBackup2B 100
# TestBackup2B -> which testcase you want to execute
# 100 -> how many times you want to execute

if [ -z "$1" ] || [ -z "$2" ]; then
  echo 'Usage:'
  echo '   example:  ./loop_to_test_raft.sh 2A 4'
  echo '   100 -> how many times you want to execute'
fi

for ((i = 1; i <= $2; i++)); do
    start=$(date +%s)
    go test -race -run $1 > $1_out

    if [ $? -ne 0 ]; then
      end=$(date +%s)
      take=$(( end - start ))
      echo "kvserver test $1 $i failed, take time ${take}s"
      break
    else
      end=$(date +%s)
      take=$(( end - start ))
      echo "kvserver test $1 $i succeed, take time ${take}s"
    fi
    sleep 0.5
done
