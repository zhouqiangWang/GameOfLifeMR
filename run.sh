#!/usr/bin/env bash

HADOOP_HOME='/Users/mac/Downloads/hadoop-3.2.0'

if [ -d 'out' ]; then
  rm -rf out
fi

hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input ./board.txt \
  -output ./out \
  -inputformat org.apache.hadoop.mapred.KeyValueTextInputFormat \
  -mapper org.apache.hadoop.mapred.lib.IdentityMapper \
  -reducer /usr/bin/wc